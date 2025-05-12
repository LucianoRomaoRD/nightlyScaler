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

## 🚀 Uso

Execute o script principal:
```
python nightly_scaler.py
```

## 🛡️ Tratamento de Erros e Timeouts
- Timeout de 10s ao listar nós para clusters inacessíveis

- Erros de autenticação registrados como ValueError

- Falhas ao enviar logs são tratadas com handleError(record) sem interromper a execução

## Próximos Passos
- Permitir a execução do script passando apenas um contexto como argumento.
- Configurar scrpt para ser executado via cron no cluster rd-devops
- Thresholds customizáveis via CLI ou arquivo de configuração