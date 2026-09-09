#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Micro-medições do overhead do ProcessPoolExecutor (apoio à Etapa 4).

Mede separadamente:
  1) custo de criar e encerrar o pool de processos (fork + inicialização);
  2) custo de despachar/coletar uma tarefa trivial (serialização + IPC);
  3) efeito do chunksize sobre esse custo de despacho.
"""

import statistics
import time
from concurrent.futures import ProcessPoolExecutor


def nada(x):
    """Tarefa trivial: só devolve o valor recebido (custo de CPU ~ zero)."""
    return x


def custo_criacao(workers, repeticoes=5):
    tempos = []
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        with ProcessPoolExecutor(max_workers=workers) as executor:
            executor.submit(nada, 0).result()   # garante que os processos subiram
        tempos.append(time.perf_counter() - inicio)
    return statistics.median(tempos)


def custo_despacho(workers, n, chunksize, repeticoes=5):
    tempos = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        executor.submit(nada, 0).result()      # aquece o pool
        for _ in range(repeticoes):
            inicio = time.perf_counter()
            list(executor.map(nada, range(n), chunksize=chunksize))
            tempos.append(time.perf_counter() - inicio)
    return statistics.median(tempos) / n


if __name__ == "__main__":
    print("Custo de criar/encerrar o pool (inclui subir os processos):")
    for workers in (2, 4, 8):
        print(f"  {workers} processos: {custo_criacao(workers) * 1000:7.2f} ms  "
              f"({custo_criacao(workers) / workers * 1000:5.2f} ms por processo)")

    print()
    print("Custo médio de enviar 1 tarefa ao pool e receber o resultado")
    print("(tarefa sem trabalho útil -> mede só serialização + IPC + agendamento):")
    for chunk in (1, 5, 25):
        custo = custo_despacho(4, 1000, chunk)
        print(f"  4 processos, chunksize={chunk:<3}: {custo * 1e6:7.1f} us por tarefa")
