# Análise Interativa de Prestações de Contas Eleitorais - 2024

Este repositório contém o código-fonte de um dashboard interativo, cujo propósito é a análise e visualização de dados referentes a despesas de campanhas eleitorais no Brasil para o ano de 2024. O projeto foi desenvolvido com o objetivo de prover uma ferramenta cívica que facilite a fiscalização e promova a transparência e a boa governança, através da exploração sistemática dos dados públicos.

## Principais Características

  * **Painel Interativo:** Interface composta por 11 módulos de análise pré-configurados, permitindo a exploração de diferentes facetas dos dados.
  * **Processo ETL Automatizado:** Implementação de um pipeline de Extração, Transformação e Carga (ETL) para a ingestão, limpeza e estruturação dos dados brutos em um banco de dados relacional.
  * **Filtragem Dinâmica:** Interface com múltiplos filtros para a segmentação de dados por critérios geográficos (estado), temporais (intervalo de datas) e de valor.
  * **Visualização de Dados:** Geração de gráficos e tabelas interativas por meio da biblioteca Plotly para facilitar a interpretação dos resultados.

## Estrutura

A aplicação foi desenvolvida utilizando as seguintes tecnologias:

  * **Linguagem de Programação:** Python 3
  * **Framework de Interface Web:** Streamlit
  * **Biblioteca de Análise de Dados:** Pandas
  * **Sistema de Banco de Dados:** SQLite
  * **Biblioteca de Visualização:** Plotly

## Instruções de Execução

Para executar a aplicação em um ambiente local, siga os procedimentos abaixo.

1.  **Clonagem do Repositório:**

    ```bash
    git clone [URL_DO_REPOSITÓRIO]
    cd [NOME_DA_PASTA_DO_PROJETO]
    ```

2.  **Configuração do Ambiente Virtual:**
    Recomenda-se fortemente a utilização de um ambiente virtual para isolar as dependências do projeto.

    ```bash
    # Criação do ambiente
    python -m venv venv

    # Ativação no sistema operacional Windows
    .\venv\Scripts\activate

    # Ativação em sistemas operacionais baseados em Unix (Linux/macOS)
    source venv/bin/activate
    ```

3.  **Instalação das Dependências:**
    As bibliotecas necessárias estão listadas no arquivo `requirements.txt`.

    ```bash
    pip install -r requirements.txt
    ```

4.  **Inicialização da Aplicação:**
    Com o ambiente ativado, execute o servidor do Streamlit.

    ```bash
    streamlit run app.py
    ```

    A aplicação será inicializada e estará acessível através de um endereço local (`http://localhost:8501`) em seu navegador web.

## Dependências do Projeto

O arquivo `requirements.txt` deve conter as seguintes bibliotecas:

```
streamlit
pandas
gdown
plotly
```
