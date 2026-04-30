cd C:\Projects\forex_system\services\collector_events

.\.venv\Scripts\Activate.ps1

python -m pip install -U pip

pip install lingua-language-detector fasttext-wheel huggingface_hub transformers sentencepiece accelerate sacremoses

pip install torch --index-url https://download.pytorch.org/whl/cpu

mkdir .models\lid -Force

Invoke-WebRequest `
  -Uri "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin" `
  -OutFile ".models\lid\lid.176.bin"


mkdir .models\nllb-200-distilled-600M -Force

python -c "from huggingface_hub import snapshot_download; snapshot_download(repo_id='facebook/nllb-200-distilled-600M', local_dir='.models/nllb-200-distilled-600M', allow_patterns=['*.json','*.model','tokenizer.json','pytorch_model.bin','README.md'])"
