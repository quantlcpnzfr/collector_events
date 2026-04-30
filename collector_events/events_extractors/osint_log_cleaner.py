import re
import os
from pathlib import Path

def clean_telegram_dashboard_log(file_path: str):
    """
    Remove blocos do log do dashboard que não possuem texto na mensagem.
    """
    path = Path(file_path)
    if not path.exists():
        print(f"❌ Arquivo não encontrado: {file_path}")
        return

    print(f"🧹 Iniciando limpeza de: {file_path}")

    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    # O log usa uma linha de 96 caracteres como separador de blocos
    separator = "────────────────────────────────────────────────────────────────────────────────────────────────"
    
    # Dividimos o conteúdo em blocos. 
    # O primeiro bloco costuma ser o cabeçalho (Dashboard Header)
    parts = content.split(separator)
    
    cleaned_parts = []
    
    # Preservamos o cabeçalho inicial (geralmente a primeira parte até o primeiro separador)
    if len(parts) > 0:
        cleaned_parts.append(parts[0])

    entries_removed = 0
    entries_kept = 0

    for block in parts[1:]:
        # Ignora blocos que são apenas espaços em branco
        if not block.strip():
            continue

        # Procuramos o campo "💬 Message" e o texto que o segue
        # A regex captura tudo após "💬 Message" até o final do bloco
        match = re.search(r"💬 Message\s*\n\s*(.*)", block, re.DOTALL)
        
        if match:
            message_text = match.group(1).strip()
            
            # Se a mensagem tiver conteúdo real, mantemos o bloco
            if message_text and len(message_text) > 0:
                cleaned_parts.append(block)
                entries_kept += 1
            else:
                entries_removed += 1
        else:
            # Se não encontrar o campo message (ex: blocos de status/saúde), mantemos por segurança
            cleaned_parts.append(block)

    # Reconstrói o arquivo
    new_content = separator.join(cleaned_parts)

    # Salvamos em um novo arquivo para segurança, ou podemos sobrescrever
    output_path = path.with_name(path.stem + "_cleaned" + path.suffix)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"✅ Limpeza concluída!")
    print(f"📊 Entradas mantidas: {entries_kept}")
    print(f"🗑️  Entradas vazias removidas: {entries_removed}")
    print(f"💾 Arquivo salvo em: {output_path}")

if __name__ == "__main__":
    # Ajuste o caminho se necessário
    log_file = "telegram_osint_dashboard.log"
    clean_telegram_dashboard_log(log_file)