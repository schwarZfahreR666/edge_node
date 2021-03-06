CURRENT_DIR=`pwd`
export BERT_BASE_DIR=$CURRENT_DIR/prev_trained_model/bert-base
export DATA_DIR=$CURRENT_DIR/datasets
export OUTPUR_DIR=$CURRENT_DIR/outputs
TASK_NAME="cluener"

python run_ner_span.py \
  --model_type=bert \
  --model_name_or_path=$BERT_BASE_DIR \
  --task_name=$TASK_NAME \
  --overwrite_cache \
  --do_train \
  --do_predict \
  --do_adv \
  --do_lower_case \
  --loss_type=ce \
  --data_dir=$DATA_DIR/${TASK_NAME}/ \
  --train_max_seq_length=128 \
  --eval_max_seq_length=512 \
  --per_gpu_train_batch_size=12 \
  --per_gpu_eval_batch_size=12 \
  --learning_rate=2e-5 \
  --num_train_epochs=100.0 \
  --logging_steps=448 \
  --save_steps=448 \
  --output_dir=$OUTPUR_DIR/${TASK_NAME}_output/ \
  --overwrite_output_dir \
  --seed=42

