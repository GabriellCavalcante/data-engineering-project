# Guia de Arquitetura — Template de Engenharia de Dados Python/PySpark

Este projeto é um template para pipelines de Engenharia de Dados em Python/PySpark. Mais importante do que a árvore de diretórios é o raciocínio arquitetural por trás dela: cada pacote representa uma responsabilidade, uma fronteira técnica ou uma etapa clara do fluxo.

> **Ideia central:** antes de criar uma classe ou função, pergunte: **qual responsabilidade ela representa e quem deveria conhecer essa responsabilidade?** A resposta determina o pacote correto e ajuda a evitar acoplamento desnecessário.

## Sumário

- [1. Modelo mental](#1-modelo-mental)
- [2. Estrutura principal](#2-estrutura-principal)
- [3. Como os pacotes se relacionam](#3-como-os-pacotes-se-relacionam)
- [4. Responsabilidade de cada pacote](#4-responsabilidade-de-cada-pacote)
- [5. Ordem de composição no `main.py`](#5-ordem-de-composição-no-mainpy)
- [6. Ordem de execução dentro do `Service`](#6-ordem-de-execução-dentro-do-service)
- [7. Como decidir onde colocar um novo código](#7-como-decidir-onde-colocar-um-novo-código)
- [8. Data IO e S3](#8-data-io-e-s3)
- [9. Load control, API e banco de controle](#9-load-control-api-e-banco-de-controle)
- [10. Business, Data Quality e Queries](#10-business-data-quality-e-queries)
- [11. Conceitos de engenharia de software](#11-conceitos-de-engenharia-de-software)
- [12. Como evoluir um projeto real](#12-como-evoluir-um-projeto-real)
- [13. Testes](#13-testes)
- [14. Antipadrões](#14-antipadrões)
- [15. Checklist antes de abrir um PR](#15-checklist-antes-de-abrir-um-pr)
- [16. Resumo final](#16-resumo-final)

---

## 1. Modelo mental

A melhor forma de entender a arquitetura deste template é:

> **O `main.py` monta os objetos; o `Service` define a ordem; cada componente executa apenas a sua responsabilidade.**

Existem duas sequências diferentes que não devem ser confundidas:

| Sequência | Onde ocorre | Finalidade |
|---|---|---|
| **Composição** | `batch/main.py` | Criar, configurar e conectar as dependências da aplicação. |
| **Execução** | `service/service.py` | Chamar os componentes na ordem correta durante o processamento. |

Essa distinção é fundamental. O `main.py` responde principalmente **quais objetos serão usados?**. O `Service` responde **em qual ordem esses objetos serão chamados?**.

---

## 2. Estrutura principal

```text
data-engineering-project/
├── README.md
├── pyproject.toml
├── .gitignore
│
├── config/
│   └── settings.yaml
│
├── src/
│   └── company_name/
│       ├── __init__.py
│       │
│       └── project_name/
│           ├── __init__.py
│           │
│           ├── batch/
│           │   ├── __init__.py
│           │   └── main.py
│           │
│           ├── config/
│           │   ├── __init__.py
│           │   └── environment/
│           │       ├── __init__.py
│           │       └── settings.py
│           │
│           ├── service/
│           │   ├── __init__.py
│           │   └── service.py
│           │
│           ├── business/
│           │   ├── __init__.py
│           │   └── processor.py
│           │
│           ├── load_control/
│           │   ├── __init__.py
│           │   └── load_control.py
│           │
│           ├── data_io/
│           │   ├── __init__.py
│           │   ├── readers/
│           │   │   ├── __init__.py
│           │   │   ├── csv_reader.py
│           │   │   ├── parquet_reader.py
│           │   │   └── delta_reader.py
│           │   ├── writers/
│           │   │   ├── __init__.py
│           │   │   ├── csv_writer.py
│           │   │   ├── parquet_writer.py
│           │   │   └── delta_writer.py
│           │   └── schemas/
│           │       └── __init__.py
│           │
│           ├── queries/
│           │   ├── __init__.py
│           │   └── queries.py
│           │
│           ├── infrastructure/
│           │   ├── __init__.py
│           │   ├── spark/
│           │   │   ├── __init__.py
│           │   │   └── spark_session_factory.py
│           │   ├── connectors/
│           │   │   ├── __init__.py
│           │   │   └── api/
│           │   │       ├── __init__.py
│           │   │       └── api_client.py
│           │   └── secrets/
│           │       ├── __init__.py
│           │       └── secrets_manager.py
│           │
│           ├── data_quality/
│           │   ├── __init__.py
│           │   └── base_validator.py
│           │
│           ├── common/
│           │   ├── __init__.py
│           │   ├── constants/
│           │   │   ├── __init__.py
│           │   │   └── application_constants.py
│           │   └── exceptions/
│           │       ├── __init__.py
│           │       └── processing_exception.py
│           │
│           └── utils/
│               └── __init__.py
│
└── tests/
    ├── unit/
    └── integration/
```

---

## 3. Como os pacotes se relacionam

O pipeline acessa arquivos no Amazon S3 **por meio do Spark**. Para o controle de carga, o projeto conhece somente a API; o banco utilizado pelo backend fica escondido atrás da API e não é uma dependência direta do pipeline.

```text
config/settings.yaml
        │
        ▼
   batch/main.py
        │
        │ composição das dependências
        ▼
      Service
        │
        ├────► LoadControl
        │          │
        │          ▼
        │       ApiClient
        │          │
        │          ▼
        │      API Gateway / Backend
        │          │
        │          ▼
        │     persistência de controle
        │
        ├────► Reader
        │          │
        │          ▼
        │        Spark
        │          │
        │          ▼
        │          S3
        │
        ├────► Data Quality
        │
        ├────► Business
        │
        └────► Writer
                   │
                   ▼
                 Spark
                   │
                   ▼
                   S3
```

### 3.1 Tipos de pacote

| Tipo | Pacotes | Como pensar |
|---|---|---|
| **Fluxo principal** | `batch`, `service`, `load_control`, `data_io`, `data_quality`, `business` | Participam diretamente da composição ou da execução do pipeline. |
| **Infraestrutura** | `infrastructure/spark`, `infrastructure/connectors/api`, `infrastructure/secrets` | Escondem detalhes técnicos de runtime e integrações externas. |
| **Apoio** | `config`, `queries`, `common`, `utils`, `schemas` | Fornecem configuração, SQL, constantes, exceções, schemas e helpers; não definem a sequência principal. |

> **Consequência prática:** não crie um `mysql_client.py` neste projeto se todo o acesso ao controle de carga continuar sendo feito pela API. A persistência atrás da API é um detalhe externo ao pipeline.

---

## 4. Responsabilidade de cada pacote

| Pacote | Papel | Pode conter | Evite |
|---|---|---|---|
| `batch/` | Ponto de entrada e **composition root**. | Instanciar Settings, Spark, readers/writers, ApiClient, LoadControl, Validator, Processor e Service. Chamar `Service.run()`. | Transformações e regras de negócio. |
| `config/` | Configuração externa. | Carregar `settings.yaml` e expor valores. | Espalhar leitura de YAML em várias classes. |
| `service/` | Orquestração. | Definir a ordem das etapas e coordenar sucesso/falha. | Detalhes de HTTP, Spark IO ou regra de negócio. |
| `business/` | Regra de negócio e transformações. | Filtrar, derivar, agrupar, combinar e aplicar semântica de negócio. | Decidir se a carga pode iniciar ou chamar API de controle. |
| `load_control/` | Regra sistêmica de execução. | Autorizar carga, registrar início, sucesso e falha. | Conhecer banco de dados diretamente. |
| `data_io/` | Entrada/saída de DataFrames Spark. | Ler/gravar CSV, Parquet e Delta a partir de paths. | Guardar paths fixos ou regra de negócio. |
| `data_quality/` | Qualidade do conteúdo dos dados. | Validar colunas, nulos, duplicidade, domínio, volume, freshness etc. | Ser confundido com controle de carga. |
| `queries/` | SQL reutilizável do projeto. | Centralizar consultas quando SQL for parte da solução. | Virar repositório de SQL de outros projetos. |
| `infrastructure/` | Detalhes técnicos externos. | SparkSession, HTTP, secrets e futuros adaptadores técnicos. | Receber lógica de negócio. |
| `common/` | Elementos compartilhados e estáveis. | Constantes realmente fixas e exceções da aplicação. | Guardar configuração de ambiente. |
| `utils/` | Helpers pequenos e genéricos. | Funções simples e coesas que não possuem dono melhor. | Virar pasta `misc` ou depósito de qualquer função. |
| `tests/` | Proteção do comportamento. | Testes unitários e de integração. | Depender apenas de testes manuais. |

---

## 5. Ordem de composição no `main.py`

O `main.py` é o ponto em que as dependências concretas são escolhidas. Esse papel é frequentemente chamado de **Composition Root**: o local em que a aplicação decide quais implementações serão conectadas entre si.

### 5.1 O que o `main.py` deve fazer

1. Carregar `Settings`.
2. Criar a `SparkSession`.
3. Escolher o `Reader` e o `Writer` de acordo com a configuração.
4. Criar o `ApiClient` somente quando o controle de carga estiver habilitado.
5. Criar o `LoadControl`.
6. Criar o Validator e o Processor do projeto.
7. Injetar tudo no `Service`.
8. Chamar `Service.run()` e finalizar a `SparkSession` em `finally`.

```text
Settings
   ↓
SparkSessionFactory
   ↓
Reader / Writer
   ↓
ApiClient
   ↓
LoadControl
   ↓
Validator / BusinessProcessor
   ↓
Service
   ↓
Service.run()
```

### 5.2 Por que o `main.py` não deve processar dados

Se o `main.py` começar a conter filtros Spark, joins, validações ou chamadas HTTP de negócio, ele deixa de ser um ponto de composição e passa a misturar responsabilidades. Isso torna os testes mais difíceis e faz a aplicação depender de uma ordem implícita espalhada em código procedural.

> **Regra:** `main.py` deve responder principalmente **“quais objetos serão usados?”**. `service.py` deve responder **“em qual ordem esses objetos serão chamados?”**.

---

## 6. Ordem de execução dentro do `Service`

Na versão padrão do template, `Service.run()` define sete passos:

```text
1. load_control.can_start(context)
        ↓
2. load_control.start(context)
        ↓
3. reader.read(input_path)
        ↓
4. validator.validate(dataframe)
        ↓
5. processor.process(dataframe)
        ↓
6. writer.write(result, output_path)
        ↓
7. load_control.mark_success(...)
```

Em caso de exceção:

```text
qualquer etapa falha
        ↓
Service captura a exceção
        ↓
load_control.mark_failure(...)
        ↓
exceção é propagada
```

### 6.1 Por que o tratamento da falha fica no `Service`

O `Service` enxerga o ciclo completo da execução. Quando uma etapa falha, ele possui contexto suficiente para registrar a falha no controle de carga e propagar a exceção. O componente que falhou não deve tentar coordenar o restante do pipeline.

---

## 7. Como decidir onde colocar um novo código

Antes de criar um arquivo, use estas perguntas:

| Pergunta | Destino mais provável |
|---|---|
| Estou apenas montando e conectando objetos? | `batch/main.py` |
| Estou definindo a sequência de etapas? | `service/` |
| A decisão depende do significado do dado para o negócio? | `business/` |
| Estou verificando se o dataset é confiável/válido? | `data_quality/` |
| Estou decidindo se a execução pode iniciar ou atualizando seu status? | `load_control/` |
| Estou lendo ou escrevendo DataFrame com Spark? | `data_io/` |
| Estou chamando HTTP, criando SparkSession ou obtendo secret? | `infrastructure/` |
| O valor muda por ambiente/path? | `config/settings.yaml` |
| É um valor realmente fixo da aplicação? | `common/constants/` |
| É uma query reutilizável? | `queries/` |
| É um helper pequeno, genérico e sem melhor dono? | `utils/` |

> **Sinal de alerta:** se uma classe precisa conhecer Spark, `requests`, regra de negócio, configuração e controle de carga ao mesmo tempo, provavelmente ela está fazendo coisas demais.

---

## 8. Data IO e S3

CSV, Parquet e Delta permanecem em `data_io` mesmo quando os arquivos estão no S3. O reader/writer representa a operação de DataFrame; o path `s3://` é apenas o endereço fornecido pela configuração.

```text
settings.yaml
  paths.input:  s3://bucket/input/
  paths.output: s3://bucket/output/

        │
        ▼
Service
   ├──► ParquetReader.read(path) ──► Spark ──► S3
   └──► ParquetWriter.write(df, path) ──► Spark ──► S3
```

### 8.1 Um reader por formato, não por dataset

O mesmo `ParquetReader` pode ser reutilizado para dezenas de paths. Não crie `customer_parquet_reader.py`, `contract_parquet_reader.py` e `payment_parquet_reader.py` apenas porque existem três datasets.

```python
reader = ParquetReader(spark)

customers = reader.read(settings.require("paths.customers"))
contracts = reader.read(settings.require("paths.contracts"))
payments = reader.read(settings.require("paths.payments"))
```

Crie um componente específico somente quando houver **comportamento de leitura realmente específico e relevante**.

### 8.2 Quando S3 entraria em `infrastructure`

Somente se o projeto passar a fazer operações diretas via `boto3`, como:

- listar objetos;
- copiar ou mover arquivos;
- excluir objetos;
- ler metadados;
- testar existência fora do Spark.

Nesse caso, faria sentido criar algo como:

```text
infrastructure/
└── storage/
    └── s3/
        └── s3_client.py
```

Enquanto o acesso ao S3 for somente via Spark, essa camada não é necessária.

---

## 9. Load control, API e banco de controle

O controle de carga é **sistêmico**: ele responde se a pipeline pode executar e registra o estado da execução. Por isso ele fica separado de `business` e `data_quality`.

| Componente | Responsabilidade |
|---|---|
| `LoadControl` | Interpretar regras do controle: pode iniciar, registrar início, sucesso e falha. |
| `ApiClient` | Executar HTTP GET/POST, timeout, headers e tratamento HTTP básico. |
| API Gateway / backend | Expor o contrato de controle para o projeto. |
| Banco de controle | Persistência interna do backend; não é conhecido diretamente pelo pipeline. |

### 9.1 Por que separar `LoadControl` de `ApiClient`

`ApiClient` sabe **como falar HTTP**, mas não deveria saber o significado de `can_start`, `mark_success` ou `mark_failure`.

`LoadControl` conhece essa semântica, mas não deveria implementar `requests.Session`.

Separar os dois aumenta coesão e permite testar a regra de controle com um cliente falso, sem rede.

### 9.2 `SecretsManager`

`SecretsManager` é um adaptador técnico opcional. Se a API exigir token, chave ou outro segredo, o `main.py` pode obter esse segredo e utilizá-lo para montar os headers do `ApiClient`.

A regra continua sendo:

> **Segredo é resolvido na borda da aplicação; regra de negócio não consulta Secrets Manager.**

---

## 10. Business, Data Quality e Queries

### 10.1 `business/`

`business/` concentra transformações que existem porque o negócio exige determinado resultado.

O `BusinessProcessor` fornecido no template é **pass-through de propósito**: isso deixa o template executável sem inventar regra específica. Em um projeto real, substitua ou especialize esse comportamento com classes coesas e nomes do domínio.

### 10.2 `data_quality/`

`BaseValidator` também é neutro no template. O projeto real deve adicionar validações que protejam contratos de dados.

Prefira validadores:

- pequenos;
- explícitos;
- testáveis;
- compostos quando houver muitas regras.

Evite transformar uma única classe em um arquivo gigantesco.

A diferença conceitual é:

| Data Quality | Load Control |
|---|---|
| **“Este DataFrame está válido?”** | **“Esta execução pode/deve acontecer?”** |
| Colunas, tipos, nulos, domínio, duplicidade, volume, freshness. | Status, concorrência, competência, idempotência operacional, início/sucesso/falha. |

### 10.3 `queries/`

`queries/` é um repositório para SQL reutilizável do próprio projeto.

Ele não precisa aparecer no fluxo padrão quando a solução não usa SQL. Quando uma transformação ou leitura precisar de SQL reutilizável, o módulo responsável pode importar a query daqui.

---

## 11. Conceitos de engenharia de software

O template se apoia em princípios conhecidos de engenharia de software, mas de forma pragmática para pipelines PySpark.

| Conceito | Como aparece no template | Benefício |
|---|---|---|
| **Separation of Concerns** | Cada pacote trata um tipo de preocupação: negócio, IO, infraestrutura, configuração, qualidade ou orquestração. | Mudanças localizadas e menor efeito cascata. |
| **Single Responsibility Principle** | Classes como `ApiClient`, `LoadControl`, Reader e Processor possuem um motivo principal para mudar. | Código mais simples de entender e testar. |
| **Dependency Injection** | `Service` recebe reader, writer, validator, processor e load_control pelo construtor. | Permite trocar implementações e usar fakes/mocks nos testes. |
| **Composition Root** | `main.py` escolhe e conecta implementações concretas. | A criação de objetos não fica espalhada pela aplicação. |
| **Dependency Inversion, de forma pragmática** | `Service` depende do comportamento esperado dos componentes e não precisa conhecer `ParquetReader`, `ApiClient` etc. | Orquestração menos acoplada aos detalhes. |
| **High Cohesion / Low Coupling** | Código relacionado fica junto; módulos evitam conhecer detalhes que não precisam. | Facilita evolução e manutenção. |
| **Externalized Configuration** | Paths, formatos, endpoints e parâmetros de ambiente ficam em `settings.yaml`. | Evita hardcode e facilita dev/hml/prod. |
| **Boundary / Adapter thinking** | Spark, HTTP e Secrets Manager ficam nas bordas técnicas. | Regra de negócio não depende diretamente de tecnologia externa. |
| **Testability by design** | A ordem do `Service` pode ser testada com objetos falsos, sem Spark ou rede. | Falhas arquiteturais aparecem cedo em testes unitários. |

> O template é inspirado nesses princípios, mas **não tenta implementar uma Clean Architecture ou Hexagonal Architecture formal com todas as suas camadas e interfaces**. Essa escolha é deliberadamente pragmática para pipelines PySpark.

---

## 12. Como evoluir um projeto real

Ao iniciar um novo projeto com este template:

1. Renomeie `project_name` para o nome Python do projeto em `snake_case` e ajuste o entry point no `pyproject.toml`.
2. Preencha `config/settings.yaml` com nome da aplicação, paths S3, formatos, opções Spark e endpoints de load control.
3. Escolha/reutilize os readers e writers existentes. Não crie um arquivo por dataset sem necessidade.
4. Implemente as transformações específicas dentro de `business/`. Prefira classes pequenas por responsabilidade do domínio.
5. Implemente regras de data quality dentro de `data_quality/`.
6. Adicione schemas explícitos quando forem necessários para leitura, contratos ou testes.
7. Adicione SQL reutilizável em `queries/` somente quando o projeto realmente usar SQL.
8. Mantenha o controle de carga em `load_control/` e a comunicação HTTP em `infrastructure/connectors/api/`.
9. Altere `Service` somente quando a sequência real do pipeline precisar mudar.
10. Altere `main.py` somente quando a composição/dependências precisarem mudar.
11. Crie testes unitários para regras e para a sequência; crie testes de integração para Spark/IO e integrações que precisem ser exercitadas em conjunto.

### 12.1 Se houver várias entradas

Há duas opções saudáveis:

**Pipelines simples:** o `Service` pode receber o mesmo reader e chamar `read()` para vários paths configurados.

**Pipelines maiores:** crie um Service específico do caso de uso, com nomes claros, e mantenha o reader genérico.

O que deve ser evitado é duplicar leitores apenas porque os paths mudam.

### 12.2 Se o `Service` crescer demais

O `Service` deve orquestrar, mas não virar um arquivo de centenas de linhas contendo lógica de transformação.

Quando o fluxo tiver subetapas complexas:

- extraia componentes de negócio;
- extraia validadores;
- extraia serviços de caso de uso quando necessário;
- mantenha no `Service` apenas a coordenação de alto nível.

---

## 13. Testes

O template possui um teste que registra cada chamada e confirma a ordem do `Service`. Esse tipo de teste é valioso porque funciona como **documentação executável** da arquitetura.

A ordem esperada é:

```text
load_control.can_start
load_control.start
reader.read
validator.validate
processor.process
writer.write
load_control.mark_success
```

Se alguém inverter etapas importantes ou esquecer `mark_success`, o teste deve falhar.

### 13.1 Pirâmide recomendada

| Nível | O que testar |
|---|---|
| **Unitário** | Business, validadores, LoadControl, Settings e ordem do Service usando doubles/fakes. |
| **Integração** | Transformações com Spark local, leitura/escrita de formatos e contratos de integração quando aplicável. |
| **Ponta a ponta** | Fluxo completo em ambiente controlado quando custo e risco justificarem. |

---

## 14. Antipadrões

Evite:

- colocar paths S3 em `common/constants`; paths variam por ambiente e pertencem à configuração;
- criar um reader Python para cada Parquet quando a única diferença é o path;
- chamar API Gateway diretamente de `business/`;
- colocar regras de `can_start` e status dentro de `ApiClient`;
- fazer Spark transformations em `main.py`;
- colocar regra de negócio no `Service` em vez de delegar para `business/`;
- usar `data_quality` para decidir se uma carga pode iniciar; isso pertence a `load_control`;
- acessar o banco de controle diretamente se o contrato oficial do projeto é via API;
- criar uma camada S3/boto3 sem existir necessidade de acesso direto fora do Spark;
- transformar `utils/` em depósito de funções sem dono;
- esconder configuração dentro de classes concretas, tornando testes e ambientes acoplados.

---

## 15. Checklist antes de abrir um PR

- [ ] O arquivo foi colocado no pacote que corresponde à sua responsabilidade?
- [ ] A classe tem um motivo principal para mudar?
- [ ] `main.py` continua sendo apenas composição/startup?
- [ ] `Service` continua mostrando claramente a ordem do pipeline?
- [ ] Regra de negócio está fora de `infrastructure` e `data_io`?
- [ ] Paths, endpoints e opções variáveis estão em `settings.yaml`?
- [ ] Reader/Writer recebe path/config e permanece reutilizável?
- [ ] `LoadControl` usa `ApiClient` e não conhece banco de dados diretamente?
- [ ] Falhas relevantes chegam ao `Service` para `mark_failure`?
- [ ] Existe teste unitário para a nova regra?
- [ ] Se a ordem do fluxo mudou, o teste de sequência foi atualizado conscientemente?
- [ ] Foi evitada uma nova camada/pasta sem necessidade real?

---

## 16. Resumo final

A melhor forma de pensar ao desenvolver neste template é:

1. **Comece pela responsabilidade, não pela tecnologia.**
2. **Mantenha o fluxo visível:** `main` monta; `Service` orquestra; componentes executam.
3. **Deixe detalhes externos nas bordas:** Spark, HTTP e Secrets Manager.
4. **Negócio e qualidade devem ser testáveis** sem depender de rede ou de como os objetos foram criados.
5. **Prefira reutilizar componentes genéricos** e crie especializações somente quando existir comportamento específico real.
6. **Não adicione camadas preventivamente.** A estrutura deve crescer quando a complexidade real exigir.

### Fluxo mental recomendado

```text
Configurar
    ↓
Compor
    ↓
Orquestrar
    ↓
Ler
    ↓
Validar
    ↓
Transformar
    ↓
Gravar
    ↓
Registrar estado
```

Essa sequência deve permanecer fácil de enxergar no código. Quando ela deixa de estar clara, normalmente é sinal de que alguma responsabilidade foi colocada no lugar errado ou que um componente começou a acumular funções demais.
