## Objetivo

Construir um projeto de portfólio que demonstre um pipeline de Engenharia de Dados de ponta a ponta, usando Python, SQL, PySpark, Airflow, armazenamento em arquivos e banco relacional.

O projeto deve ser desenvolvido de forma incremental, com foco em código compreensível, testável e próximo de práticas utilizadas em ambientes reais. Não implemente todas as tecnologias de uma vez: priorize uma base funcional e evolua por etapas.

## Contexto do desenvolvedor

- Formação em Ciência da Computação.
- Experiência prática com Python, Pandas, SQL, APIs REST, PostgreSQL, Docker e Git.
- Já desenvolveu aplicações backend e projetos próprios.
- Está aprofundando conhecimentos em Engenharia de Dados.
- Prefere explicações diretas, exemplos práticos e pouca abstração desnecessária.
- Não precisa de explicações sobre sintaxe básica quando já demonstrar domínio; explique conceitos novos e decisões de arquitetura.

## Arquitetura desejada

Pipeline conceitual:

1. **Ingestão:** coletar dados de uma API pública usando Python.
2. **Raw/Bronze:** preservar os dados recebidos, com metadados de ingestão.
3. **Transformação/Silver:** validar, limpar, normalizar e tratar duplicidades com PySpark.
4. **Agregação/Gold:** gerar tabelas analíticas e indicadores úteis.
5. **Persistência:** armazenar dados processados em Parquet/Delta e disponibilizar resultados em PostgreSQL quando fizer sentido.
6. **Orquestração:** usar Apache Airflow para agendar e controlar as etapas.
7. **Observabilidade:** registrar execuções, erros, duração e volume processado.
8. **Execução local:** utilizar Docker e Docker Compose para facilitar a reprodução.

A arquitetura pode ser simplificada no início. Não adicione serviços ou ferramentas sem uma necessidade clara.

## Stack-alvo

- Python 3
- Pandas
- PySpark
- SQL
- PostgreSQL
- Apache Airflow
- Parquet
- Delta Lake (etapa posterior)
- Docker / Docker Compose
- Pytest
- Git e GitHub

Azure ou GCP podem ser adicionados posteriormente. Não tornar o projeto dependente de serviços pagos ou de infraestrutura em nuvem para funcionar localmente.

## Plano incremental

### Etapa 1 — Fundamentos do pipeline
- Escolher uma API pública estável e documentada.
- Implementar cliente HTTP com timeout, tratamento de erros e paginação, se aplicável.
- Salvar resposta bruta em JSON.
- Registrar data/hora de coleta e origem dos dados.
- Criar configuração por variáveis de ambiente.
- Escrever testes para a ingestão.

**Critério de conclusão:** executar a ingestão localmente e produzir arquivos brutos reproduzíveis.

### Etapa 2 — SQL e modelagem
- Definir entidades e granularidade dos dados.
- Criar tabelas PostgreSQL.
- Praticar JOIN, CTE, funções de janela, agregações e tratamento de duplicidades.
- Documentar as decisões de modelagem.

**Critério de conclusão:** carregar dados e consultar resultados com SQL.

### Etapa 3 — Transformação com Pandas e PySpark
- Implementar validações de esquema e campos obrigatórios.
- Tratar valores ausentes, tipos, duplicidades e registros inválidos.
- Criar transformações reproduzíveis.
- Comparar, quando útil, a abordagem Pandas com PySpark.
- Salvar dados tratados em Parquet.

**Critério de conclusão:** executar a transformação sobre os dados brutos e gerar saídas validadas.

### Etapa 4 — Orquestração
- Criar DAGs no Airflow.
- Separar ingestão, transformação e carga em tarefas.
- Configurar dependências, retries, timeouts e logs.
- Evitar colocar toda a lógica de negócio diretamente dentro da DAG.

**Critério de conclusão:** executar o pipeline pelo Airflow e identificar falhas por tarefa.

### Etapa 5 — Qualidade e operação
- Adicionar testes unitários e de integração com Pytest.
- Validar contagem de registros, unicidade, campos obrigatórios e consistência.
- Implementar logging estruturado.
- Documentar como reprocessar uma execução sem duplicar dados.
- Adicionar Docker Compose e instruções de execução.

**Critério de conclusão:** outra pessoa consegue subir o ambiente, executar testes e rodar o pipeline seguindo o README.

### Etapa 6 — Evolução
Somente depois das etapas anteriores:
- Introduzir Delta Lake e arquitetura Medallion completa.
- Avaliar Databricks.
- Avaliar Kafka caso exista um caso real de streaming.
- Avaliar Azure ou GCP para implantação.
- Adicionar monitoramento visual se houver métricas suficientes.

## Diretrizes de implementação

1. Antes de alterar arquivos, inspecione a estrutura e leia o README e as configurações existentes.
2. Trabalhe em etapas pequenas e funcionais. Não gere uma aplicação inteira sem validar os requisitos.
3. Explique brevemente o plano antes de mudanças relevantes.
4. Não invente endpoints, campos, formatos ou comportamentos da API. Consulte a documentação ou examine respostas reais.
5. Não inclua credenciais, tokens ou dados sensíveis no código. Use `.env` e mantenha `.env` no `.gitignore`.
6. Forneça `.env.example` sem segredos.
7. Prefira funções pequenas, nomes claros, type hints e responsabilidades bem separadas.
8. Evite abstrações, dependências e padrões arquiteturais que não tragam benefício concreto.
9. Trate erros de rede, respostas inválidas, limites de requisição e reexecuções.
10. Não descarte os dados brutos: eles devem permitir auditoria e reprocessamento.
11. Garanta idempotência sempre que possível: executar novamente uma etapa não deve gerar duplicações indevidas.
12. Não esconda erros com `except` genérico. Registre contexto e propague falhas quando necessário.
13. Escreva testes para regras de transformação e casos de erro.
14. Atualize o README quando adicionar comandos, variáveis de ambiente, serviços ou decisões arquiteturais.
15. Não afirme que algo foi testado sem realmente executar os testes ou comandos correspondentes.

## Organização sugerida

Adapte à necessidade real do projeto, sem criar diretórios vazios antecipadamente:

```text
.
├── dags/
├── data/
│   ├── raw/
│   ├── processed/
│   └── gold/
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── database/
│   └── common/
├── tests/
├── scripts/
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

Os arquivos de dados locais devem ser ignorados pelo Git, salvo pequenos exemplos sintéticos criados para documentação e testes.

## Como trabalhar comigo pelo CLI

- Atue como parceiro de desenvolvimento e tutor técnico.
- Primeiro entenda o código existente; preserve decisões válidas.
- Quando eu pedir uma implementação, apresente uma solução objetiva e explique os pontos importantes.
- Se houver mais de uma abordagem, recomende uma e explique o motivo, sem listar alternativas desnecessárias.
- Se faltar uma informação essencial, faça uma pergunta direta em vez de presumir.
- Não reescreva partes não relacionadas ao pedido.
- Não faça mudanças destrutivas ou migrações sem explicar o impacto e pedir confirmação quando necessário.
- Ao concluir uma tarefa, informe: arquivos alterados, comportamento implementado, comandos executados e testes/resultados.
- Sugira o próximo passo somente quando ele for realmente necessário.

## Primeiro trabalho

Não comece instalando todas as ferramentas.

1. Inspecione o repositório.
2. Resuma o que já existe e o que está faltando.
3. Proponha um escopo pequeno para a Etapa 1.
4. Sugira uma API pública adequada, justificando a escolha e verificando sua documentação.
5. Aguarde minha confirmação antes de criar a estrutura ou implementar.