# Scripts-CMMaia

## Proposta

### Gestão e Monitorização de Dados Municipais: Plataforma de Dados Abertos, Consumos de Energia e de Água

#### Context
A Câmara Municipal da Maia adoptou a plataforma OpenDataSoft como plataforma para Gestão do Acesso a Dados, seja dos seus indicadores internos seja dos seus conjuntos de Dados Abertos. Os dados (e os metadados respectivos) são alimentados à plataforma OpenDataSoft a partir do data lake da Câmara Municipal da Maia, utilizando os mecanismos disponíveis na plataforma OpenDataSoft (configuração manual na plataforma, utilização de REST API). Pretende-se, com esta proposta: Melhorar as actuais práticas de operação da plataforma OpenDataSoft @ CMMaia; Testar diferentes alternativas seja para a actualizaçao de dados seja para a actualização de metadados; Testar e propôr mecanismos de verificação e becnhmarking da qualidade e coerência dos metadados; Garantir o alinhamento com recomendações Europeias / Nacionais (ARTEAMA, INE) Monitorização da eficácias das soluções adoptadas.

#### Objectives
Identificar características-chave da operação da plataforma OpenDataSoft @ CMMaia; Efectuar uma revisão bibliográfica de estratégias de gestão de dados alinhada com a plataforma OpenDataSoft Projectar e conduzir testes piloto de adoção de alternativas de gestão / revisão / actualização de metadados e reportar criticamente os respectivos resultados

#### Innovation
Automatização das tarefas de edição / harmonização / actualização de metadados Alinhamento das funcionalidades da plataforma OpenDataSoft com recomendações UE, Nacionais (AMA, INE) ou de outros organismos de normalização (ISO, IDSA, etc)

#### Workplan
1- Revisão de Literatura e Estado da Arte
2 - Definição dos testes
3 - Implementação dos testes
4 - Análise de resultados e desenvolvimento da proposta em prova de conceito
5 - Escrita da dissertação

#### Bibliography
https://dl.acm.org/doi/abs/10.1145/2964909
https://dl.acm.org/doi/abs/10.1145/3560107.3560142
https://www.liebertpub.com/doi/abs/10.1089/big.2014.0020
https://dl.acm.org/doi/abs/10.1145/3409795

#### Profile
Competências prévias em programação Python para Data Engineering; Familiaridade com REST API Interesse pela área de Metadados e normalização

## Metodologia

- `import_to_aiven.py`: localiza ficheiros `.csv` e `.json` na pasta `data`, reconhece documentos GeoJSON e a estrutura específica da cache da BaZe, valida os dados e importa-os para Aiven. Cria as tabelas ausentes, recarrega as tabelas compatíveis e guarda o resultado de cada grupo na tabela `import_log`.
- `notebooks/database_to_spreadsheet.ipynb`: exporta as tabelas do catálogo existentes na Aiven para folhas do Google Sheets, permitindo a sua consulta e revisão pelos técnicos municipais.
- `notebooks/spreadsheet_to_database.ipynb`: compara as folhas revistas com as tabelas de origem e, quando existem alterações, guarda as versões confirmadas em tabelas com o sufixo `_confirmed` na Aiven e no CrateDB.
- `database_to_HuWise.py`: lê uma tabela configurada na Aiven e implementa a criação ou atualização do dataset, do recurso e dos metadados na Huwise. O teste completo da publicação está pendente por falta das permissões necessárias na plataforma.
- `database_to_dados_gov.py`: exporta uma tabela da Aiven para CSV ou GeoJSON, valida o utilizador, a organização, a licença e a frequência e cria ou atualiza o dataset, o recurso e os metadados no dados.gov.pt. Os identificadores devolvidos pela plataforma ficam guardados em `dados_gov_tables.json` para permitir atualizações posteriores.
- `huwise_tables.json` e `dados_gov_tables.json`: definem as tabelas e os parâmetros específicos de cada publicação.
- `.env`: contém as credenciais e os endereços dos serviços. Este ficheiro não deve ser incluído no controlo de versões; `.env.example` documenta as variáveis necessárias.

Para instalar as dependências e executar os componentes de linha de comandos:

```bash
python -m pip install -r requirements.txt
python import_to_aiven.py
python database_to_HuWise.py baze_cache_energia
python database_to_dados_gov.py estacoes_percursos
```

O script de importação processa automaticamente os grupos encontrados em `data`. Os scripts de publicação recebem como argumento o nome de uma tabela previamente definida no respetivo ficheiro de configuração.

## TODO

- Testar a criação ou atualização do dataset na Huwise através do `database_to_HuWise.py`, atualmente pendente por falta das permissões necessárias na plataforma.

## Scripts no colab

Os notebooks funcionais `notebooks/database_to_spreadsheet.ipynb` e `notebooks/spreadsheet_to_database.ipynb` são executados no Google Colab. As credenciais da Aiven, do CrateDB e do Google são obtidas através do mecanismo de segredos do Colab e não devem ser escritas diretamente no código.

O notebook `notebooks/database_to_HuWise.ipynb` é mantido apenas como registo do protótipo inicial. A publicação na Huwise está implementada no script Python `database_to_HuWise.py`, mas o seu teste completo está pendente por falta das permissões necessárias na plataforma.
