# FinOps Nightly Scaler

Este repositório contém um script Python para realizar scale-down automático de node-pools Kubernetes em horários de baixa demanda (nightly), além de enviar logs estruturados diretamente para a API REST de Logs do Datadog.

## 📋 Visão Geral
- Descobre quantos nós estão provisionados em cada context do Kubernetes

- Calcula quantos nós podem ser drenados com base em utilização de CPU, memória e contagem de pods

- Agrupa verificações por contexto em threads/paralelismo para acelerar a execução

- Envia logs detalhados (info, warning, error) para o Datadog via endpoint HTTP v2 (sem Agent)

## 🛠 Pré-requisitos
- Python 3.10+ (testado em 3.13)
- Acesso ao ~/.kube/config com contexts válidos
- Variáveis de ambiente do Datadog:
    * DD_API_KEY_RDSM
    * DD_APP_KEY_RDSM
    * DD_SITE (ex: us5.datadoghq.com)

## 📦 Instalação

Clone o repositório:
```
git clone https://seu-repositorio/finops-nightly-scaler.git
cd finops-nightly-scaler
```

Crie e ative um ambiente virtual:
```
python -m venv .venv
source .venv/bin/activate
```

Instale as dependências:
```
pip install -r requirements.txt
```

## ⚙️ Configuração
Defina as seguintes variáveis de ambiente:
* DD_API_KEY_RDSM	Chave de API do Datadog para Logs	obrigatório
* DD_APP_KEY_RDSM	App Key do Datadog (endpoint v2 de Logs)	obrigatório
* DD_SITE	Região do Datadog	datadoghq.com
* PROJECT_IDS - Lista dos projetos da GCP que o script deve ter acesso para remover as VM. o nome dos projetos deve estar separado por virgula. Ex: "projeto1,projeto2,projeto3"

## 🚀 Uso

Execute o script principal:
```
python nightly_scaler.py
```
## 🛡️ Informações de segurança

Para a listagem e removação de VMS está sendo utilizada uma SA o qual foi criada manualmente no projeto rd-cloudfinops já que o mesmo não se encontra devidamente configurado no terraform.

Posteriormente essa SA teve as permissões adicionada em todos os projetos pertinentes. 

Caso haja erros de autenticação, por favor, verificar as configurações da SA.

## 🛡️ Tratamento de Erros e Timeouts
- Timeout de 10s ao listar nós para clusters inacessíveis

- Erros de autenticação registrados como ValueError

- Falhas ao enviar logs são tratadas com handleError(record) sem interromper a execução

## Próximos Passos
- Permitir a execução do script passando apenas um contexto como argumento.
- Configurar scrpt para ser executado via cron no cluster rd-devops
- Thresholds customizáveis via CLI ou arquivo de configuração
- Refatoração para ter exceções pŕoprias ao invés de usar RuntimeError