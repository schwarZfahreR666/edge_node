
import os

import torch
from torch.utils.data import DataLoader, SequentialSampler, TensorDataset



from edge_node.apps.ner.tools.common import seed_everything


from edge_node.apps.ner.models.transformers import BertConfig
from edge_node.apps.ner.models.bert_for_ner import BertSpanForNer
from edge_node.apps.ner.processors.utils_ner import CNerTokenizer
from edge_node.apps.ner.processors.ner_span import convert_examples_to_features, CluenerProcessor, InputExample
from edge_node.apps.ner.processors.ner_span import ner_processors as processors
from edge_node.apps.ner.processors.ner_span import collate_fn
from edge_node.apps.ner.processors.utils_ner import bert_extract_item

_change_status_root = os.path.join('edge_node/apps', 'ner')

model_type = 'bert'
model_name_or_path= _change_status_root + '/prev_trained_model/bert-base'
task_name="cluener"
data_dir= _change_status_root + '/datasets/cluener/'
train_max_seq_length=128
eval_max_seq_length=512
per_gpu_train_batch_size=12
per_gpu_eval_batch_size=12
learning_rate=2e-5
num_train_epochs=200.0
logging_steps=448
save_steps=448
output_dir= _change_status_root + '/outputs/cluener_output/bert'
seed=42

def input_process(text,tokenizer,label_list):
    lines = []
    words = list(text)
    labels = ['O'] * len(words)
    lines.append({"words": words, "labels": labels})
    examples = []
    for (i, line) in enumerate(lines):
        guid = "%s-%s" % ('test', i)
        text_a = line['words']
        labels = line['labels']
        subject = []
        examples.append(InputExample(guid=guid, text_a=text_a, subject=subject))

    features = convert_examples_to_features(examples=examples,
                                            tokenizer=tokenizer,
                                            label_list=label_list,
                                            max_seq_length=eval_max_seq_length,
                                            cls_token_at_end=bool(model_type in ["xlnet"]),
                                            pad_on_left=bool(model_type in ['xlnet']),
                                            cls_token=tokenizer.cls_token,
                                            cls_token_segment_id=2 if model_type in ["xlnet"] else 0,
                                            sep_token=tokenizer.sep_token,
                                            # pad on the left for xlnet
                                            pad_token=tokenizer.convert_tokens_to_ids([tokenizer.pad_token])[0],
                                            pad_token_segment_id=4 if model_type in ['xlnet'] else 0,
                                            )
    all_input_ids = torch.tensor([f.input_ids for f in features], dtype=torch.long)
    all_input_mask = torch.tensor([f.input_mask for f in features], dtype=torch.long)
    all_segment_ids = torch.tensor([f.segment_ids for f in features], dtype=torch.long)
    all_start_ids = torch.tensor([f.start_ids for f in features], dtype=torch.long)
    all_end_ids = torch.tensor([f.end_ids for f in features], dtype=torch.long)
    all_input_lens = torch.tensor([f.input_len for f in features], dtype=torch.long)
    dataset = TensorDataset(all_input_ids, all_input_mask, all_segment_ids, all_start_ids, all_end_ids, all_input_lens)
    return dataset





def load_and_cache_examples(task, tokenizer, data_type='train'):
     # Make sure only the first process in distributed training process the dataset, and the others will use the cache
    processor = processors[task]()
    # Load data features from cache or dataset file
    cached_features_file = os.path.join(data_dir, 'cached_span-{}_{}_{}_{}'.format(
        data_type,
        list(filter(None, model_name_or_path.split('/'))).pop(),
        str(train_max_seq_length if data_type == 'train' else eval_max_seq_length),
        str(task)))

    label_list = processor.get_labels()

    examples = processor.get_test_examples(data_dir)
    features = convert_examples_to_features(examples=examples,
                                             tokenizer=tokenizer,
                                             label_list=label_list,
                                             max_seq_length=train_max_seq_length if data_type == 'train' \
                                                 else eval_max_seq_length,
                                             cls_token_at_end=bool(model_type in ["xlnet"]),
                                             pad_on_left=bool(model_type in ['xlnet']),
                                             cls_token=tokenizer.cls_token,
                                             cls_token_segment_id=2 if model_type in ["xlnet"] else 0,
                                             sep_token=tokenizer.sep_token,
                                             # pad on the left for xlnet
                                             pad_token=tokenizer.convert_tokens_to_ids([tokenizer.pad_token])[0],
                                             pad_token_segment_id=4 if model_type in ['xlnet'] else 0,
                                             )

    torch.save(features, cached_features_file)

    all_input_ids = torch.tensor([f.input_ids for f in features], dtype=torch.long)
    all_input_mask = torch.tensor([f.input_mask for f in features], dtype=torch.long)
    all_segment_ids = torch.tensor([f.segment_ids for f in features], dtype=torch.long)
    all_start_ids = torch.tensor([f.start_ids for f in features], dtype=torch.long)
    all_end_ids = torch.tensor([f.end_ids for f in features], dtype=torch.long)
    all_input_lens = torch.tensor([f.input_len for f in features], dtype=torch.long)
    dataset = TensorDataset(all_input_ids, all_input_mask, all_segment_ids, all_start_ids, all_end_ids, all_input_lens)
    return dataset


def init():
    '''
    初始化模型
    '''
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    seed_everything(seed)
    processor = CluenerProcessor()
    label_list = processor.get_labels()   #  ["O", "address", "time","company",'action','government','movie','name','organization','position','scene']
    id2label = {i: label for i, label in enumerate(label_list)} # 两个映射字典{0:'0',1:'address'}
    num_labels = len(label_list)
    config_class, model_class, tokenizer_class = BertConfig,BertSpanForNer,CNerTokenizer
    # BertConfig.from_pretrained()  读配置文件
    config = config_class.from_pretrained(model_name_or_path,
                                          num_labels=num_labels,
                                          loss_type='ce',
                                          cache_dir=None,
                                          soft_label=True)  # 读取config.json

    # BertSpanForNer.from_pretrained()  读取预训练模型
    model = model_class.from_pretrained(model_name_or_path,
                                        from_tf=bool(".ckpt" in model_name_or_path),
                                        config=config)

    tokenizer = tokenizer_class.from_pretrained(output_dir, do_lower_case=True)
    checkpoints = [output_dir]

    for checkpoint in checkpoints:
        model = model_class.from_pretrained(checkpoint)
        model.to(device)

    return tokenizer,label_list,model,device,id2label



def predict(input_text,tokenizer,label_list,model,device,id2label):



    '''
    事件处理
    '''
    # test_dataset = load_and_cache_examples(task_name, tokenizer, data_type='test')
    test_dataset = input_process(input_text,tokenizer,label_list)
    # Note that DistributedSampler samples randomly
    test_sampler = SequentialSampler(test_dataset)
    test_dataloader = DataLoader(test_dataset, sampler=test_sampler, batch_size=1, collate_fn=collate_fn)
    results = []
    # pbar = ProgressBar(n_total=len(test_dataloader), desc="Predicting")
    for step, batch in enumerate(test_dataloader):
        model.eval()
        batch = tuple(t.to(device) for t in batch)
        with torch.no_grad():
            inputs = {"input_ids": batch[0], "attention_mask": batch[1], "start_positions": None, "end_positions": None}
            outputs = model(**inputs)
        start_logits, end_logits = outputs[:2]
        R = bert_extract_item(start_logits, end_logits)
        if R:
            label_entities = [[id2label[x[0]], x[1], x[2]] for x in R]
        else:
            label_entities = []
        json_d = {}
        json_d['id'] = step
        json_d['entities'] = label_entities
        results.append(json_d)
        # pbar(step)
    test_text = []
    test_text.append(input_text)
    for x, y in zip(test_text, results):
        json_d = {}
        json_d['label'] = {}
        entities = y['entities']
        words = list(x)
        if len(entities) != 0:
            for subject in entities:
                tag = subject[0]
                start = subject[1]
                end = subject[2]
                word = "".join(words[start:end + 1])
                if tag in json_d['label']:
                    if word in json_d['label'][tag]:
                        json_d['label'][tag][word].append([start, end])
                    else:
                        json_d['label'][tag][word] = [[start, end]]
                else:
                    json_d['label'][tag] = {}
                    json_d['label'][tag][word] = [[start, end]]
            return json_d




if __name__ == "__main__":
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    input_text = "决定2020年8月12日至2020年9月10日期间，半壁街（厂洼中路——西三环北路）禁止社会车辆及行人通行，"
    input_text = "宫门口西岔(安平巷—阜成门内大街)采取禁止机动车由南向北方向行驶交通管理措施。"
    predict(input_text)