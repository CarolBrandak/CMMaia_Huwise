# Scripts-CMMaia

## Proposta

### Gestão de uma plataforma de Dados Abertos - caso OpenDataSoft 

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

#### Notas
Entretanto o nome da plataforma OpenDataSoft mudou para Huwise.

## Metodologia 
- Script Base de dados para Spreadsheet - O script database_to_spreadsheet tranforma os dados que estão na base de dados para uma apreadsheet. A base de dados está em Aiven.
- Script SpreadSheet para Base de dados - O script spreadsheet_to_database reescreve na base de dados os dados que foram alterados. Escreve em duas base de dados diferentes uma em Aiven e outra em CreatDB
- Spreadsheet serve para os tecnicos da camara munincipal da maia confirmarem se os dados estão de acordo com os acordos DCAT-AP e ENTI
- Entretanto ao longo do periodo da dissertação foi me dado novas tarefas.
- Uma das tarefas que me foram pedidas foi uma proposta de como adicionar uns novos 500 contadores de agua da SMAS

## TODO
- Script Base de dados para HuWise - O script database_to_HuWise deverá publicar os dados e metadados que estão na base de dados no HuWise
- Script Base de dados para dados.gov.pt - Ainda é não iniciado

## Scripts no colab 
Os scripts database_to_spreadsheet, spreadsheet_to_database e database_to_HuWise estão no colab partilhado com o Prof. Pedro Pimenta.
Para guardar as pass e os logins na base de dados uso os secrets.