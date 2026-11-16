# Reconstrução de backups divididos

Backups maiores que 40 MiB são enviados ao Supabase como um manifesto JSON e
partes de até 40 MiB. Backups antigos e arquivos menores permanecem inalterados.
O manifesto é enviado depois de todas as partes; o histórico somente indica
sucesso quando o envio é concluído.

Para reconstruir um arquivo, localize o caminho do manifesto no histórico de
backups do SiSFarm. No Supabase Storage, baixe o manifesto e todas as partes
indicadas nele. Coloque as partes, sem renomeá-las, em um mesmo diretório local.
Em seguida, na raiz do projeto SiSFarm, execute:

```sh
python scripts/reconstruir_backup.py /caminho/manifest.json /caminho/partes /caminho/backup.sql.gz
```

Para anexos, escolha um nome de saída com extensão `.zip`. O comando não
sobrescreve arquivos existentes, confere tamanho e SHA-256 de cada parte e do
arquivo final e apaga sua saída incompleta se houver erro. A reconstrução não
restaura o banco automaticamente: a importação do dump deve ser feita
separadamente, após validação, no ambiente apropriado.

O limite de arquivo global e o limite do bucket precisam aceitar arquivos de
até 40 MiB. Se o Supabase estiver configurado com um limite menor, ajuste-o antes
de executar o backup. A reserva de armazenamento e a retenção continuam
aplicadas; remover backups antigos não altera o limite por arquivo.
