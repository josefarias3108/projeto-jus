📊 Projeto de Análise Jurídica - Processos Judiciais
https://img.shields.io/badge/Power-BI-yellow?style=flat&logo=powerbi
https://img.shields.io/badge/Python-3.x-blue?style=flat&logo=python
https://img.shields.io/badge/PostgreSQL-15-blue?style=flat&logo=postgresql

Sistema completo de análise de dados jurídicos para insights estratégicos em processos judiciais.

🎯 Objetivo do Projeto
Analisar quantitative e qualitativamente processos judiciais para identificar padrões, otimizar estratégias jurídicas e compreender o impacto da conciliação nos resultados.

📈 Principais Métricas Analisadas
Volume Processual: Quantidade de processos por juiz, advogado e parte

Desempenho Financeiro: Saldo de causas ganhas vs. perdidas por advogado

Impacto da Conciliação: Eficácia dos processos conciliados vs. não conciliados

Distribuição Demográfica: Análise por faixa etária e perfil das partes

Performance por Especialidade: Resultados por tipo de processo e vara

🏗️ Arquitetura do Projeto
Diagram
Code
![deepseek_mermaid_20250820_c193b2](https://github.com/user-attachments/assets/526136e5-b54b-475c-aeab-8d2db58b5fcf)
![FLUXO DO PROJETO](https://github.com/user-attachments/assets/6e70fb51-d17a-45b6-832e-178fd01416f9)






📊 Modelo Dimensional
Tabelas Principais
Dimensões:

dim_pessoa - Partes envolvidas (autores e réus)

dim_advogado - Dados profissionais dos advogados

dim_juiz - Informações dos magistrados

d_calendario - Dimensão temporal

Fato:

fato_processos - Métricas e eventos dos processos

⚡ Medidas DAX Principais
dax
// Exemplo de medidas críticas
Saldo por Advogado = [Valor Ganha por Advogado] - [Valor Perdido por Advogado]
% Ganho com Conciliação = DIVIDE([Ganho c/ conciliação], [Total causa ganha], 0)
Qtd Processos por Faixa = CALCULATE(DISTINCTCOUNT(fato_processos[numero_do_processo]), ...)
🛠️ Tecnologias Utilizadas
Python 3.x: Pipeline ETL com validação de dados

PostgreSQL 15: Armazenamento e gestão dos dados

Power BI: Visualização e análise interativa

Git: Controle de versão e colaboração

📋 Funcionalidades do Pipeline
Extração Automática: Conexão dinâmica com PostgreSQL

Validação Rigorosa: Tratamento de duplicatas e nulos

Logs Detalhados: Auditoria completa em log_extractions

Exportação Segura: Geração de CSV para Power BI

Relatórios Consolidadas: Estatísticas de qualidade dos dados

🎨 Design do Dashboard
Paleta de Cores:

Texto: #d9d9d9

Fundo: #080016

Autores: #5B6B00

Réus: #AB222C

Indicadores Negativos: #9A0814

🚀 Como Executar
bash
# Clone o repositório
git clone https://github.com/josefarias3108/projeto-jus.git

# Execute o pipeline
python pipeline_juridico.py

# Abra o dashboard Power BI
projeto-jus.pbix
📊 Principais Insights Gerados
Taxa de Sucesso: Advogados com +50 casos têm 30% melhor performance

Conciliação: Processos conciliados têm 65% maior taxa de ganho

Perfil Etário: Faixa 35-50 anos concentra 45% dos autores

Valor Médio: Causas conciliadas têm valor 20% menor mas maior sucesso

👥 Autor
José Augusto Palermo de Farias
Analista de Dados Jurídicos
Data: 31/07/2025

📄 Licença
Este projeto é para fins educacionais e de análise jurídica.

⚠️ Nota: Dados sensíveis como CPF são tratados com conformidade à LGPD através de padrões rigorosos de governança.

📅 SLA: Atualização semanal com resposta a falhas em até 24 horas.
