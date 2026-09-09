#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESTUDO DIRIGIDO 01 - Computação Paralela e Distribuída
Atividade Prática 01 - Experimento de Processamento Sequencial e Paralelo

Aplicação única que executa todas as etapas da atividade:

  ETAPA 1 - Implementação sequencial (mede T1)
  ETAPA 2 - Implementação paralela com ProcessPoolExecutor (mede Tp)
  ETAPA 3 - Registro dos resultados (tabela: tempo, speedup, eficiência, overhead)
  ETAPA 5 - Experimento de granularidade (20, 100 e 500 tarefas)
  ETAPA 6 - Desafio de balanceamento de carga (tarefas desbalanceadas +
            comparação entre distribuição estática e dinâmica)

Uso:
    python3 ed01_computacao_paralela.py                 # roda tudo
    python3 ed01_computacao_paralela.py --etapa 1       # só a etapa 1
    python3 ed01_computacao_paralela.py --repeticoes 5  # 5 execuções por medição
    python3 ed01_computacao_paralela.py --saida ./out   # onde gravar CSV/JSON

Requisitos: apenas a biblioteca padrão do Python 3.
"""

import argparse
import csv
import json
import math
import os
import platform
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

# ---------------------------------------------------------------------------
# Parâmetros globais do experimento
# ---------------------------------------------------------------------------

LIMITE_PADRAO = 300_000      # iterações de uma tarefa "normal"
LIMITE_PESADO = 1_500_000    # iterações de uma tarefa "pesada" (Etapa 6)
LISTAS_DE_PROCESSOS = [2, 4, 8]
NUCLEOS = os.cpu_count() or 1

# Coletor de resultados para gravar em CSV/JSON no final
REGISTROS = []


# ---------------------------------------------------------------------------
# ETAPA 1 - Tarefa computacionalmente intensiva (CPU-bound)
# ---------------------------------------------------------------------------

def tarefa(numero):
    """Tarefa CPU-bound: soma de (numero * i) % 97 para i em [1, 300000)."""
    resultado = 0
    for i in range(1, LIMITE_PADRAO):
        resultado += (numero * i) % 97
    return resultado


def executar_sequencial(dados, funcao=None):
    """Executa todas as tarefas em um único processo, uma após a outra."""
    funcao = funcao or tarefa
    inicio = time.perf_counter()

    resultados = []
    for numero in dados:
        resultados.append(funcao(numero))

    fim = time.perf_counter()
    return resultados, fim - inicio


# ---------------------------------------------------------------------------
# ETAPA 2 - Implementação paralela com múltiplos processos
# ---------------------------------------------------------------------------

def executar_paralelo(dados, workers, funcao=None, chunksize=1):
    """Executa as tarefas em paralelo usando um pool de processos.

    chunksize=1  -> distribuição dinâmica (cada processo pega a próxima tarefa
                    livre assim que termina a anterior).
    chunksize=k  -> distribuição em blocos de k tarefas (mais estática, menos
                    comunicação, porém pior balanceamento).
    """
    funcao = funcao or tarefa
    inicio = time.perf_counter()

    with ProcessPoolExecutor(max_workers=workers) as executor:
        resultados = list(executor.map(funcao, dados, chunksize=chunksize))

    fim = time.perf_counter()
    return resultados, fim - inicio


# ---------------------------------------------------------------------------
# ETAPA 6 - Tarefa desbalanceada e versões instrumentadas
# ---------------------------------------------------------------------------

def tarefa_desbalanceada(numero):
    """Tarefa em que múltiplos de 10 custam 5x mais que as demais entradas."""
    if numero % 10 == 0:
        limite = LIMITE_PESADO
    else:
        limite = LIMITE_PADRAO

    resultado = 0
    for i in range(1, limite):
        resultado += (numero * i) % 97

    return resultado


def tarefa_medida(numero):
    """Igual à tarefa_desbalanceada, mas devolve telemetria de execução.

    Retorna (numero, resultado, pid, instante_inicio, instante_fim).
    Em Linux, perf_counter() usa CLOCK_MONOTONIC, cuja origem é a mesma em
    todos os processos - por isso os instantes de processos diferentes são
    comparáveis e permitem reconstruir a ocupação de cada worker.
    """
    inicio = time.perf_counter()
    resultado = tarefa_desbalanceada(numero)
    fim = time.perf_counter()
    return (numero, resultado, os.getpid(), inicio, fim)


def tarefa_fatia(args):
    """Calcula apenas um pedaço (fatia) do laço de uma tarefa.

    Permite quebrar uma tarefa grande em várias tarefas menores
    (granularidade mais fina) para melhorar o balanceamento de carga.
    Retorna (numero, resultado_parcial, pid, inicio, fim).
    """
    numero, i_inicio, i_fim = args
    inicio = time.perf_counter()
    resultado = 0
    for i in range(i_inicio, i_fim):
        resultado += (numero * i) % 97
    fim = time.perf_counter()
    return (numero, resultado, os.getpid(), inicio, fim)


def gerar_fatias(dados, fatias_por_tarefa):
    """Quebra cada tarefa em 'fatias_por_tarefa' subtarefas de faixas iguais."""
    unidades = []
    for numero in dados:
        limite = LIMITE_PESADO if numero % 10 == 0 else LIMITE_PADRAO
        passo = math.ceil((limite - 1) / fatias_por_tarefa)
        i = 1
        while i < limite:
            unidades.append((numero, i, min(i + passo, limite)))
            i += passo
    return unidades


def tarefa_ajustavel(args):
    """Mesma conta da tarefa original, porém com custo parametrizável.

    Recebe (numero, limite) e permite montar experimentos em que o TRABALHO
    TOTAL é constante, mudando apenas o tamanho de cada tarefa (granularidade).
    """
    numero, limite = args
    resultado = 0
    for i in range(1, limite):
        resultado += (numero * i) % 97
    return resultado


# ---------------------------------------------------------------------------
# Utilitários de medição e apresentação
# ---------------------------------------------------------------------------

def medir(func, repeticoes, *args, **kwargs):
    """Executa a medição 'repeticoes' vezes e devolve (tempos, mediana, resultado)."""
    tempos = []
    resultados = None
    for _ in range(repeticoes):
        resultados, tempo = func(*args, **kwargs)
        tempos.append(tempo)
    return tempos, statistics.median(tempos), resultados


def registrar(**campos):
    REGISTROS.append(campos)


def titulo(texto):
    print()
    print("=" * 78)
    print(texto)
    print("=" * 78)


def sub(texto):
    print()
    print("-" * 78)
    print(texto)
    print("-" * 78)


def linha_tabela(colunas, larguras):
    return " | ".join(str(c).ljust(w) for c, w in zip(colunas, larguras))


def fmt(valor, casas=4):
    return f"{valor:.{casas}f}".replace(".", ",")


# ---------------------------------------------------------------------------
# ETAPA 1
# ---------------------------------------------------------------------------

def etapa1(dados, repeticoes):
    titulo("ETAPA 1 - IMPLEMENTAÇÃO SEQUENCIAL (medição de T1)")
    print(f"Tarefas: {len(dados)}  |  Iterações por tarefa: {LIMITE_PADRAO:,}")
    print(f"Execuções: {repeticoes}")
    print()

    tempos, mediana, resultados = medir(executar_sequencial, repeticoes, dados)
    for i, t in enumerate(tempos, start=1):
        print(f"  Execução {i}: {fmt(t)} s")

    print()
    print(f"  Menor tempo : {fmt(min(tempos))} s")
    print(f"  Maior tempo : {fmt(max(tempos))} s")
    print(f"  Média       : {fmt(statistics.mean(tempos))} s")
    print(f"  MEDIANA (T1): {fmt(mediana)} s   <-- valor usado como T1")
    print()
    print(f"  Amostra de resultados: {resultados[:3]} ...")

    registrar(etapa="1", experimento="sequencial", tarefas=len(dados),
              processos=1, tempo=mediana, speedup=1.0, eficiencia=1.0,
              overhead=0.0)
    return mediana


# ---------------------------------------------------------------------------
# ETAPAS 2 e 3
# ---------------------------------------------------------------------------

def etapas_2_e_3(dados, repeticoes, tempo_sequencial):
    titulo("ETAPAS 2 e 3 - IMPLEMENTAÇÃO PARALELA E REGISTRO DOS RESULTADOS")
    print(f"Núcleos disponíveis na máquina: {NUCLEOS}")
    print(f"Tarefas: {len(dados)}  |  T1 (mediana) = {fmt(tempo_sequencial)} s")

    larguras = [12, 14, 12, 12, 14, 14]
    cabecalho = ["Processos", "Tempo (s)", "Speedup", "Eficiência",
                 "Overhead (s)", "Tempo ideal"]
    print()
    print(linha_tabela(cabecalho, larguras))
    print("-" * (sum(larguras) + 3 * (len(larguras) - 1)))
    print(linha_tabela(["1", fmt(tempo_sequencial), "1,00", "100,0%", "0,0000",
                        fmt(tempo_sequencial)], larguras))

    for workers in LISTAS_DE_PROCESSOS:
        tempos, tempo_paralelo, _ = medir(executar_paralelo, repeticoes,
                                          dados, workers)
        speedup = tempo_sequencial / tempo_paralelo
        eficiencia = speedup / workers
        tempo_ideal = tempo_sequencial / workers
        overhead = tempo_paralelo - tempo_ideal

        print(linha_tabela([workers, fmt(tempo_paralelo), fmt(speedup, 2),
                            f"{eficiencia * 100:.1f}%".replace(".", ","),
                            fmt(overhead), fmt(tempo_ideal)], larguras))

        registrar(etapa="2-3", experimento="paralelo", tarefas=len(dados),
                  processos=workers, tempo=tempo_paralelo, speedup=speedup,
                  eficiencia=eficiencia, overhead=overhead,
                  tempos_brutos=[round(t, 4) for t in tempos])

    print()
    print("Fórmulas:  Speedup = T1 / Tp        Eficiência = Speedup / N")
    print("           Overhead = Tp - (T1 / N)  (tempo perdido em relação ao ideal)")


# ---------------------------------------------------------------------------
# ETAPA 5 - Granularidade
# ---------------------------------------------------------------------------

def etapa5(repeticoes):
    titulo("ETAPA 5 - EXPERIMENTO DE GRANULARIDADE")
    print("Mesma tarefa, quantidades diferentes de tarefas na lista de entrada.")

    experimentos = [
        ("A", list(range(1, 21))),
        ("B", list(range(1, 101))),
        ("C", list(range(1, 501))),
    ]

    larguras = [11, 10, 12, 14, 12, 12, 16]
    cabecalho = ["Experimento", "Tarefas", "Processos", "Tempo (s)",
                 "Speedup", "Eficiência", "Tarefas/proc."]

    for nome, dados in experimentos:
        sub(f"Experimento {nome} - dados = list(range(1, {len(dados) + 1}))  "
            f"({len(dados)} tarefas)")

        _, t1, _ = medir(executar_sequencial, repeticoes, dados)
        print(linha_tabela(cabecalho, larguras))
        print("-" * (sum(larguras) + 3 * (len(larguras) - 1)))
        print(linha_tabela([nome, len(dados), 1, fmt(t1), "1,00", "100,0%",
                            len(dados)], larguras))

        registrar(etapa="5", experimento=f"granularidade-{nome}",
                  tarefas=len(dados), processos=1, tempo=t1, speedup=1.0,
                  eficiencia=1.0, overhead=0.0)

        for workers in LISTAS_DE_PROCESSOS:
            _, tp, _ = medir(executar_paralelo, repeticoes, dados, workers)
            speedup = t1 / tp
            eficiencia = speedup / workers
            print(linha_tabela([nome, len(dados), workers, fmt(tp),
                                fmt(speedup, 2),
                                f"{eficiencia * 100:.1f}%".replace(".", ","),
                                fmt(len(dados) / workers, 1)], larguras))

            registrar(etapa="5", experimento=f"granularidade-{nome}",
                      tarefas=len(dados), processos=workers, tempo=tp,
                      speedup=speedup, eficiencia=eficiencia,
                      overhead=tp - t1 / workers)

        print()
        print(f"  Custo médio por tarefa (sequencial): "
              f"{fmt(t1 / len(dados) * 1000, 2)} ms")

    granularidade_trabalho_constante(repeticoes)


def granularidade_trabalho_constante(repeticoes):
    """Mantém o trabalho total fixo e varia o tamanho de cada tarefa.

    É o teste que isola o conceito de granularidade: se o resultado piora
    quando as tarefas ficam menores, a causa só pode ser o custo de
    distribuir e gerenciar as tarefas (overhead), não o volume de cálculo.
    """
    trabalho_total = 30_000_000       # iterações somadas em todos os casos
    workers = min(4, max(NUCLEOS, 2))
    configuracoes = [100, 1_000, 10_000, 50_000]

    sub("Experimento D - TRABALHO TOTAL CONSTANTE (30.000.000 iterações), "
        f"variando o tamanho da tarefa  |  {workers} processos")

    larguras = [10, 16, 13, 13, 10, 12, 18]
    print(linha_tabela(["Tarefas", "Iter./tarefa", "Tempo seq.", "Tempo par.",
                        "Speedup", "Eficiência", "Overhead/tarefa"], larguras))
    print("-" * (sum(larguras) + 3 * (len(larguras) - 1)))

    for n_tarefas in configuracoes:
        limite = trabalho_total // n_tarefas + 1
        dados = [((i % 100) + 1, limite) for i in range(n_tarefas)]

        _, t1, _ = medir(executar_sequencial, repeticoes, dados, tarefa_ajustavel)
        _, tp, _ = medir(executar_paralelo, repeticoes, dados, workers,
                         tarefa_ajustavel)

        speedup = t1 / tp
        eficiencia = speedup / workers
        overhead_por_tarefa = (tp - t1 / workers) / n_tarefas

        print(linha_tabela([f"{n_tarefas:,}".replace(",", "."),
                            f"{limite:,}".replace(",", "."),
                            fmt(t1) + " s", fmt(tp) + " s", fmt(speedup, 2),
                            f"{eficiencia * 100:.1f}%".replace(".", ","),
                            f"{overhead_por_tarefa * 1e6:.1f} us"], larguras))

        registrar(etapa="5", experimento="granularidade-trabalho-constante",
                  tarefas=n_tarefas, processos=workers, tempo=tp,
                  speedup=speedup, eficiencia=eficiencia,
                  overhead=tp - t1 / workers, iteracoes_por_tarefa=limite,
                  tempo_sequencial=t1,
                  overhead_por_tarefa_us=overhead_por_tarefa * 1e6)

    print()
    print("  Todas as linhas realizam exatamente o mesmo cálculo total.")
    print("  A única diferença é em quantos pedaços esse cálculo foi dividido.")


# ---------------------------------------------------------------------------
# ETAPA 6 - Balanceamento de carga
# ---------------------------------------------------------------------------

def executar_com_telemetria(funcao, entrada, workers, chunksize):
    """Executa em paralelo coletando telemetria (pid e instantes) de cada unidade."""
    inicio = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as executor:
        telemetria = list(executor.map(funcao, entrada, chunksize=chunksize))
    return telemetria, time.perf_counter() - inicio


def medir_telemetria(funcao, entrada, workers, chunksize, repeticoes):
    """Repete a execução e devolve a execução MEDIANA (telemetria + tempo)."""
    execucoes = [executar_com_telemetria(funcao, entrada, workers, chunksize)
                 for _ in range(repeticoes)]
    execucoes.sort(key=lambda execucao: execucao[1])
    telemetria, tempo = execucoes[len(execucoes) // 2]
    return telemetria, tempo, [round(e[1], 4) for e in execucoes]


def relatorio_ocupacao(telemetria, tempo_total, workers):
    """Mostra quanto cada processo ficou ocupado e quanto ficou ocioso."""
    ocupacao = {}
    quantidade = {}
    for _, _, pid, ini, fim in telemetria:
        ocupacao[pid] = ocupacao.get(pid, 0.0) + (fim - ini)
        quantidade[pid] = quantidade.get(pid, 0) + 1

    print()
    print(f"    {'PID':<10}{'Unidades':<11}{'Ocupado (s)':<15}"
          f"{'Ocioso (s)':<14}{'Utilização':<12}")
    for pid in sorted(ocupacao, key=lambda p: -ocupacao[p]):
        ocupado = ocupacao[pid]
        ocioso = max(tempo_total - ocupado, 0.0)
        print(f"    {pid:<10}{quantidade[pid]:<11}{fmt(ocupado):<15}"
              f"{fmt(ocioso):<14}{ocupado / tempo_total * 100:>6.1f}%")

    tempos = list(ocupacao.values())
    if len(tempos) < workers:
        print(f"    (obs.: apenas {len(tempos)} processos receberam tarefas, "
              f"de {workers} disponíveis)")

    desbalanceamento = (max(tempos) - min(tempos)) if len(tempos) > 1 else 0.0
    ociosidade_total = sum(max(tempo_total - t, 0.0) for t in tempos)
    print()
    print(f"    Processo mais carregado  : {fmt(max(tempos))} s")
    print(f"    Processo menos carregado : {fmt(min(tempos))} s")
    print(f"    Desbalanceamento (max-min): {fmt(desbalanceamento)} s")
    print(f"    Tempo ocioso somado       : {fmt(ociosidade_total)} s")
    print(f"    Utilização média do pool  : "
          f"{statistics.mean(tempos) / tempo_total * 100:.1f}%")
    return {
        "ocupado_max": max(tempos), "ocupado_min": min(tempos),
        "desbalanceamento": desbalanceamento,
        "ociosidade_total": ociosidade_total,
        "utilizacao_media": statistics.mean(tempos) / tempo_total,
        "processos_usados": len(tempos),
    }


def etapa6(repeticoes):
    titulo("ETAPA 6 - DESAFIO DE BALANCEAMENTO DE CARGA")
    dados = list(range(1, 101))
    workers = min(4, max(NUCLEOS, 2))
    pesadas = [n for n in dados if n % 10 == 0]
    leves = [n for n in dados if n % 10 != 0]
    dados_adverso = leves + pesadas          # as 10 pesadas agrupadas no final
    chunk_estatico = math.ceil(len(dados) / workers)

    print(f"Tarefa modificada: múltiplos de 10 usam {LIMITE_PESADO:,} iterações "
          f"({LIMITE_PESADO // LIMITE_PADRAO}x mais trabalho)")
    print(f"                   e as demais entradas, {LIMITE_PADRAO:,}.")
    print(f"Tarefas: {len(dados)}  ({len(pesadas)} pesadas, {len(leves)} leves)"
          f"  |  Processos: {workers}")
    print(f"Distribuição estática emulada com chunksize = {chunk_estatico} "
          f"(blocos contíguos); dinâmica com chunksize = 1.")

    _, t1, resultados_seq = medir(executar_sequencial, repeticoes, dados,
                                  tarefa_desbalanceada)
    print()
    print(f"  T1 (sequencial, carga desbalanceada): {fmt(t1)} s")
    registrar(etapa="6", experimento="desbalanceado-sequencial",
              tarefas=len(dados), processos=1, tempo=t1, speedup=1.0,
              eficiencia=1.0, overhead=0.0)

    cenarios = [
        ("A", "ordem original (1..100)  +  distribuição ESTÁTICA",
         dados, chunk_estatico),
        ("B", "ordem original (1..100)  +  distribuição DINÂMICA",
         dados, 1),
        ("C", "pesadas agrupadas no fim +  distribuição ESTÁTICA",
         dados_adverso, chunk_estatico),
        ("D", "pesadas agrupadas no fim +  distribuição DINÂMICA",
         dados_adverso, 1),
    ]

    resumo = []
    for codigo, rotulo, entrada, chunk in cenarios:
        sub(f"Cenário {codigo} - {rotulo}")
        telemetria, tempo, brutos = medir_telemetria(
            tarefa_medida, entrada, workers, chunk, repeticoes)
        speedup = t1 / tempo
        print(f"  Tempo total (Tp): {fmt(tempo)} s   "
              f"(execuções: {', '.join(fmt(b) for b in brutos)})")
        print(f"  Speedup: {fmt(speedup, 2)}   |   "
              f"Eficiência: {speedup / workers * 100:.1f}%")
        metricas = relatorio_ocupacao(telemetria, tempo, workers)
        resumo.append((f"{codigo}. {rotulo}", tempo, speedup, metricas))
        registrar(etapa="6", experimento=f"balanceamento-{codigo}",
                  tarefas=len(entrada), processos=workers, tempo=tempo,
                  speedup=speedup, eficiencia=speedup / workers,
                  overhead=tempo - t1 / workers, chunksize=chunk,
                  tempos_brutos=brutos, **metricas)

    # Cenário E: granularidade mais fina - cada tarefa quebrada em subtarefas
    fatias_por_tarefa = 5
    unidades = gerar_fatias(dados_adverso, fatias_por_tarefa)
    sub(f"Cenário E - pesadas agrupadas no fim + SUBTAREFAS "
        f"({fatias_por_tarefa} fatias por tarefa) + distribuição DINÂMICA")
    print(f"  Unidades de trabalho enviadas ao pool: {len(unidades)} "
          f"(antes eram {len(dados)})")

    telemetria, tempo, brutos = medir_telemetria(
        tarefa_fatia, unidades, workers, 1, repeticoes)

    # Reagrupa os resultados parciais e confere com a versão sequencial
    agregado = {}
    for numero, parcial, _, _, _ in telemetria:
        agregado[numero] = agregado.get(numero, 0) + parcial
    conferido = all(agregado[n] == r for n, r in zip(dados, resultados_seq))

    speedup = t1 / tempo
    print(f"  Tempo total (Tp): {fmt(tempo)} s   "
          f"(execuções: {', '.join(fmt(b) for b in brutos)})")
    print(f"  Speedup: {fmt(speedup, 2)}   |   "
          f"Eficiência: {speedup / workers * 100:.1f}%")
    print(f"  Resultados idênticos aos da versão sequencial: "
          f"{'SIM' if conferido else 'NÃO'}")
    metricas = relatorio_ocupacao(telemetria, tempo, workers)
    resumo.append((f"E. pesadas agrupadas + {fatias_por_tarefa} subtarefas "
                   f"+ DINÂMICA", tempo, speedup, metricas))
    registrar(etapa="6", experimento="balanceamento-E",
              tarefas=len(unidades), processos=workers, tempo=tempo,
              speedup=speedup, eficiencia=speedup / workers,
              overhead=tempo - t1 / workers, chunksize=1,
              tempos_brutos=brutos, **metricas)

    sub("Comparativo das estratégias de distribuição "
        f"(T1 = {fmt(t1)} s, {workers} processos)")
    larguras = [52, 11, 9, 12, 18, 12]
    print(linha_tabela(["Cenário", "Tempo (s)", "Speedup", "Eficiência",
                        "Desbalanc. (s)", "Ocioso (s)"], larguras))
    print("-" * (sum(larguras) + 3 * (len(larguras) - 1)))
    for rotulo, tempo_c, speedup_c, m in resumo:
        print(linha_tabela([rotulo, fmt(tempo_c), fmt(speedup_c, 2),
                            f"{speedup_c / workers * 100:.1f}%".replace(".", ","),
                            fmt(m["desbalanceamento"]),
                            fmt(m["ociosidade_total"])], larguras))


# ---------------------------------------------------------------------------
# Gravação dos resultados
# ---------------------------------------------------------------------------

def gravar(saida):
    if not REGISTROS:
        return
    os.makedirs(saida, exist_ok=True)
    caminho_csv = os.path.join(saida, "resultados_ed01.csv")
    caminho_json = os.path.join(saida, "resultados_ed01.json")

    colunas = []
    for registro in REGISTROS:
        for chave in registro:
            if chave not in colunas:
                colunas.append(chave)

    with open(caminho_csv, "w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas)
        escritor.writeheader()
        escritor.writerows(REGISTROS)

    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        json.dump({"ambiente": ambiente(), "medicoes": REGISTROS},
                  arquivo, indent=2, ensure_ascii=False)

    print()
    print(f"Resultados gravados em:\n  {caminho_csv}\n  {caminho_json}")


def ambiente():
    return {
        "python": platform.python_version(),
        "implementacao": platform.python_implementation(),
        "sistema": f"{platform.system()} {platform.release()}",
        "arquitetura": platform.machine(),
        "nucleos_logicos": NUCLEOS,
        "data": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ED 01 - Experimento de processamento sequencial e paralelo")
    parser.add_argument("--etapa", default="todas",
                        choices=["todas", "1", "2", "3", "5", "6"],
                        help="etapa a executar (padrão: todas)")
    parser.add_argument("--repeticoes", type=int, default=3,
                        help="execuções por medição; usa-se a mediana (padrão: 3)")
    parser.add_argument("--tarefas", type=int, default=100,
                        help="quantidade de tarefas das etapas 1 a 3 (padrão: 100)")
    parser.add_argument("--saida", default=".",
                        help="pasta onde gravar resultados_ed01.csv/.json")
    args = parser.parse_args()

    dados = list(range(1, args.tarefas + 1))

    titulo("ESTUDO DIRIGIDO 01 - COMPUTAÇÃO PARALELA E DISTRIBUÍDA")
    for chave, valor in ambiente().items():
        print(f"  {chave:<16}: {valor}")

    if args.etapa in ("todas", "1", "2", "3"):
        t1 = etapa1(dados, args.repeticoes)
        if args.etapa in ("todas", "2", "3"):
            etapas_2_e_3(dados, args.repeticoes, t1)

    if args.etapa in ("todas", "5"):
        etapa5(max(1, args.repeticoes - 2))

    if args.etapa in ("todas", "6"):
        etapa6(args.repeticoes)

    gravar(args.saida)
    print()
    print("Execução concluída.")


if __name__ == "__main__":
    sys.exit(main())
