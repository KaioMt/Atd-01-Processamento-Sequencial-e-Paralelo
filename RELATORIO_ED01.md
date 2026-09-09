# Estudo Dirigido 01 — Computação Paralela e Distribuída
## Atividade Prática 01 — Experimento de Processamento Sequencial e Paralelo

**Aplicação:** `ed01_computacao_paralela.py` (Python 3, apenas biblioteca padrão)
**Medições de apoio:** `medir_overhead.py`
**Saída bruta das execuções:** `execucao_completa.log`, `resultados_ed01.csv`

### Ambiente de teste

| Item | Valor |
|---|---|
| Python | 3.11.15 (CPython) |
| Sistema | Linux x86_64 |
| Núcleos lógicos | **4** |
| Execuções por medição | 3 (usa-se a **mediana**) |

> Os números deste relatório foram medidos no ambiente acima. Ao rodar em outra
> máquina os valores mudam, mas **o comportamento se repete**: o ganho cresce até
> o número de núcleos e depois estagna.

---

## ETAPA 1 — Implementação sequencial (T1)

100 tarefas, 300.000 iterações cada, um único processo:

| Execução | Tempo |
|---|---|
| 1ª | 2,0499 s |
| 2ª | 2,0554 s |
| 3ª | 2,0387 s |
| **Mediana (T1)** | **2,0499 s** |

Variação entre execuções: 0,017 s (≈ 0,8%). É baixa, o que mostra que a medição é
confiável — mas justifica repetir o teste em vez de confiar em uma única execução.

Custo médio de uma tarefa: **≈ 20 ms**. Esse valor é a referência para tudo o que vem
depois: enquanto o custo de *distribuir* uma tarefa for muito menor que 20 ms, o
paralelismo compensa.

---

## ETAPAS 2 e 3 — Implementação paralela e registro dos resultados

`ProcessPoolExecutor`, 100 tarefas, T1 = 2,0499 s.

| Nº de processos | Tempo de execução | Speedup | Eficiência | Overhead absoluto* |
|---|---|---|---|---|
| 1 | 2,0499 s | 1,00 | 100,0% | — |
| 2 | 1,0462 s | **1,96** | **98,0%** | 0,0213 s |
| 4 | 0,5453 s | **3,76** | **94,0%** | 0,0329 s |
| 8 | 0,5405 s | **3,79** | **47,4%** | 0,2842 s |

\* Overhead absoluto = Tp − (T1 / N), ou seja, quanto o tempo real ficou acima do
tempo ideal daquele número de processos.

**Fórmulas:** Speedup = T1 / Tp  |  Eficiência = Speedup / N

Visualmente (barras proporcionais ao speedup, ideal em cinza):

```
N=1  ██                        ideal 1   → obtido 1,00
N=2  ████                      ideal 2   → obtido 1,96
N=4  ████████                  ideal 4   → obtido 3,76
N=8  ████████                  ideal 8   → obtido 3,79   ← estagnou
```

---

## ETAPA 4 — Análise dos resultados

### 1) O speedup obtido foi igual ao número de processos? Justifique.

**Não.** Ficou próximo do ideal até 4 processos (1,96 de 2 e 3,76 de 4) e depois
parou de crescer: com 8 processos o speedup foi 3,79 — praticamente o mesmo de 4.

Três razões:

1. **Limite físico:** a máquina tem 4 núcleos. Speedup real só pode crescer enquanto
   existir hardware ocioso para executar de fato em paralelo. De 4 para 8 processos
   não há mais núcleos: os processos passam a **dividir** os mesmos 4 núcleos, e o
   sistema operacional apenas alterna entre eles (concorrência, não paralelismo).
2. **Overhead:** parte do tempo é gasta criando processos, serializando dados,
   enviando e recebendo tarefas — trabalho que a versão sequencial não faz.
3. **Fração sequencial (Lei de Amdahl):** montar a lista de entrada, criar o pool e
   remontar a lista de resultados ordenada acontece em um único processo e não
   acelera com mais processos.

### 2) A eficiência permaneceu próxima de 100%? Explique.

Até o número de núcleos, **sim**: 98,0% com 2 processos e 94,0% com 4. Com 8
processos ela **despencou para 47,4%**.

O motivo é aritmético e conceitual ao mesmo tempo: eficiência = Speedup / N. Ao dobrar
N de 4 para 8, o denominador dobrou, mas o speedup não mudou (3,76 → 3,79), porque não
existe mais capacidade de processamento para explorar. Cada processo passou a entregar
metade do que entregava — os recursos estavam sendo **compartilhados**, não somados.

A pequena perda que já existia com 2 e 4 processos (2% e 6%) é o overhead do próprio
mecanismo de paralelismo, quantificado na pergunta 5.

### 3) O que aconteceu com o tempo à medida que a quantidade de processos aumentou?

| Transição | Tempo | Redução |
|---|---|---|
| 1 → 2 processos | 2,0499 s → 1,0462 s | −49% (quase metade) |
| 2 → 4 processos | 1,0462 s → 0,5453 s | −48% (quase metade) |
| 4 → 8 processos | 0,5453 s → 0,5405 s | **−0,9% (praticamente nada)** |

O tempo caiu quase pela metade a cada duplicação **até 4 processos** e estabilizou
depois. Não houve piora significativa porque a tarefa é longa (20 ms) em relação ao
custo de troca de contexto — em tarefas curtas, oversubscription chega a piorar o
tempo (ver Etapa 5, Experimento D).

### 4) Em algum momento o ganho de desempenho começou a diminuir?

**Sim, exatamente ao ultrapassar 4 processos** — o número de núcleos da máquina. Esse é
o ponto de saturação. O ganho marginal por processo adicionado foi:

- do 1º ao 2º processo: +0,96 de speedup
- do 3º ao 4º processo: +0,90 (média por processo nessa faixa)
- do 5º ao 8º processo: **+0,008 por processo** (praticamente zero)

Aplicando a métrica de Karp–Flatt (fração serial estimada a partir do speedup medido,
`e = (1/S − 1/N) / (1 − 1/N)`):

| N | Fração serial estimada | Speedup máximo teórico (1/e) |
|---|---|---|
| 2 | 0,021 (2,1%) | ~48 |
| 4 | 0,021 (2,1%) | ~47 |
| 8 | **0,159 (15,9%)** | ~6 |

A fração serial estimada se mantém estável em ~2% até 4 processos (é o overhead real do
programa) e **salta para 15,9% com 8** — sinal clássico de que a perda não vem mais do
código, e sim da falta de hardware disponível.

### 5) Quais possíveis fontes de overhead estão presentes nessa aplicação?

O programa `medir_overhead.py` mede as duas principais isoladamente:

| Fonte de overhead | Custo medido | Onde aparece |
|---|---|---|
| **Criação do pool de processos** (fork + inicializar interpretador) | 7,8 ms (2 proc.) / 9,7 ms (4 proc.) / 15,2 ms (8 proc.) | uma vez por execução paralela |
| **Distribuir e coletar 1 tarefa** (pickle + pipe + agendamento), chunksize=1 | **262,8 µs por tarefa** | a cada tarefa enviada |
| Mesmo custo com chunksize=5 | 58,6 µs por tarefa | — |
| Mesmo custo com chunksize=25 | 11,0 µs por tarefa | — |

Fontes presentes na aplicação, na ordem em que ocorrem:

1. **Criação dos processos** — cada worker é um processo do SO com seu próprio
   interpretador Python e sua própria memória (~10 ms para 4 processos).
2. **Serialização (pickle) dos argumentos e dos resultados** — os dados precisam ser
   convertidos em bytes para atravessar a fronteira entre processos.
3. **Comunicação entre processos (IPC)** — tudo passa por pipes/filas do SO; não há
   memória compartilhada como aconteceria com threads.
4. **Agendamento do sistema operacional** — com 8 processos em 4 núcleos há troca de
   contexto e disputa por cache/memória.
5. **Coleta e reordenação dos resultados** — `executor.map` devolve os resultados na
   ordem original da entrada, o que exige que o processo principal aguarde e reagrupe.
6. **Encerramento do pool** — o `with` só termina quando todos os processos finalizam
   (efeito "cauda": ninguém sai antes do mais lento).
7. **Parte estritamente sequencial** — criação da lista de entrada e impressão dos
   resultados.

Com tarefas de 20 ms, o custo de despacho (0,26 ms) representa apenas ~1,3% — por isso a
eficiência ficou alta. Esse equilíbrio é justamente o tema da Etapa 5.

> **Observação sobre `ProcessPoolExecutor` vs. threads:** foram usados processos porque
> a tarefa é CPU-bound e o CPython tem o GIL (Global Interpreter Lock), que impede duas
> threads de executarem bytecode Python simultaneamente. Com threads o speedup seria
> ≈ 1. Processos pagam mais overhead, mas cada um tem seu próprio GIL e roda de verdade
> em paralelo.

---

## ETAPA 5 — Experimento de granularidade

### Experimentos A, B e C (mesma tarefa, quantidades diferentes)

| Exp. | Tarefas | Proc. | Tempo | Speedup | Eficiência |
|---|---|---|---|---|---|
| **A** | 20 | 1 | 0,3978 s | 1,00 | 100,0% |
| A | 20 | 2 | 0,2108 s | 1,89 | 94,4% |
| A | 20 | 4 | 0,1322 s | 3,01 | **75,2%** |
| A | 20 | 8 | 0,1250 s | 3,18 | 39,8% |
| **B** | 100 | 1 | 2,0024 s | 1,00 | 100,0% |
| B | 100 | 2 | 1,0248 s | 1,95 | 97,7% |
| B | 100 | 4 | 0,5282 s | 3,79 | **94,8%** |
| B | 100 | 8 | 0,5422 s | 3,69 | 46,2% |
| **C** | 500 | 1 | 10,0257 s | 1,00 | 100,0% |
| C | 500 | 2 | 5,0448 s | 1,99 | 99,4% |
| C | 500 | 4 | 2,5924 s | 3,87 | **96,7%** |
| C | 500 | 8 | 2,5695 s | 3,90 | 48,8% |

Com 4 processos, a eficiência sobe de **75,2% (20 tarefas) → 94,8% (100) → 96,7% (500)**.

### Experimento D (extra) — trabalho total constante, tarefas cada vez menores

Este teste isola o conceito: **o cálculo total é sempre o mesmo (30.000.000 iterações)**,
muda apenas em quantos pedaços ele é dividido. 4 processos, chunksize = 1.

| Tarefas | Iterações/tarefa | Tempo sequencial | Tempo paralelo | Speedup | Eficiência |
|---|---|---|---|---|---|
| 100 | 300.001 | 1,8488 s | 0,5070 s | **3,65** | 91,2% |
| 1.000 | 30.001 | 1,8622 s | 0,6386 s | 2,92 | 72,9% |
| 10.000 | 3.001 | 1,8970 s | **2,3679 s** | **0,80** | 20,0% |
| 50.000 | 601 | 1,8226 s | **13,8903 s** | **0,13** | 3,3% |

Com 50.000 tarefas minúsculas, a versão paralela ficou **7,6× mais lenta que a
sequencial**, fazendo exatamente a mesma conta.

### Respostas

**Como a quantidade de tarefas influencia o aproveitamento dos processos?**
Quanto mais tarefas (mantido o tamanho de cada uma), melhor o aproveitamento — por dois
motivos. Primeiro, o custo fixo de criar o pool (~10 ms) é diluído: representa 2,4% do
trabalho no Experimento A e 0,1% no C. Segundo, com poucas tarefas por processo a
divisão fica "granulada demais no fim": no Experimento A são apenas 5 tarefas por
processo, então basta um processo receber uma tarefa a mais para que todos os outros
fiquem esperando por ele (efeito cauda). Com 125 tarefas por processo, essa diferença
final vira ruído.

**Tarefas muito pequenas podem prejudicar o desempenho? Por quê?**
Sim, e de forma drástica. Cada tarefa enviada ao pool custa ~263 µs fixos de
serialização, comunicação e agendamento, **independentemente do tamanho dela**. Se a
tarefa demora 20 ms, esse custo é 1,3% e some no ruído. Se a tarefa demora 0,04 ms
(caso das 50.000 tarefas), o custo de gerenciar é ~7× maior que o cálculo em si (263 µs contra 37 µs): o
programa passa a maior parte do tempo empacotando dados em vez de calcular.

**O que acontece quando o custo de distribuir uma tarefa fica comparável ao custo de
processá-la?**
O paralelismo deixa de compensar e passa a atrapalhar — é o ponto em que o speedup cai
abaixo de 1. No experimento isso aparece entre 1.000 tarefas (speedup 2,92, ainda vale a
pena) e 10.000 tarefas (speedup 0,80, **já não vale**). O processo principal vira gargalo:
ele sozinho precisa serializar e despachar todas as tarefas, enquanto os 4 workers ficam
esperando trabalho. É o cenário em que "adicionar processos" só adiciona custo.

**Como isso se relaciona com granularidade?**
Granularidade é a razão entre o tempo de **computação** de uma unidade de trabalho e o
tempo de **comunicação/coordenação** dela:

- **Granularidade grossa** (tarefas grandes, poucas): pouco overhead relativo, mas risco
  de desbalanceamento e menos flexibilidade para distribuir (Experimento A).
- **Granularidade fina** (tarefas pequenas, muitas): distribuição flexível e bem
  balanceada, mas o overhead domina (Experimento D com 10.000+ tarefas).
- **Ponto ideal:** tarefas grandes o bastante para o overhead ser desprezível e em
  quantidade suficiente para manter todos os processos ocupados. No experimento, isso
  ficou entre **300.000 e 30.000 iterações por tarefa** (20 ms a 2 ms), com pelo menos
  ~25 tarefas por processo.

---

## ETAPA 6 — Desafio de balanceamento de carga

Tarefa modificada: múltiplos de 10 executam 1.500.000 iterações (5× mais trabalho); as
demais, 300.000. São 10 tarefas pesadas e 90 leves. **T1 = 2,6756 s**, 4 processos.

Foram comparadas duas estratégias de distribuição e dois cenários de ordenação dos dados:

- **estática** — `chunksize = 25`: cada processo recebe um bloco fixo e contíguo de 25
  tarefas, definido *antes* de começar;
- **dinâmica** — `chunksize = 1`: cada processo pega a próxima tarefa da fila assim que
  termina a anterior.

| Cenário | Tempo | Speedup | Eficiência | Desbalanceamento* | Ocioso total |
|---|---|---|---|---|---|
| **A** — ordem original + estática | 0,7393 s | 3,62 | 90,5% | 0,1110 s | 0,3150 s |
| **B** — ordem original + dinâmica | 0,7519 s | 3,56 | 89,0% | 0,0773 s | 0,3375 s |
| **C** — pesadas agrupadas + estática | **1,2834 s** | **2,08** | **52,1%** | **0,7826 s** | **2,4129 s** |
| **D** — pesadas agrupadas + dinâmica | 0,7674 s | 3,49 | 87,2% | 0,0944 s | 0,2838 s |
| **E** — pesadas agrupadas + 5 subtarefas + dinâmica | 0,7879 s | 3,40 | 84,9% | **0,0218 s** | 0,2421 s |

\* Desbalanceamento = tempo ocupado do processo mais carregado − o do menos carregado.

**Ocupação individual no cenário C** (o pior caso), medida processo a processo:

| PID | Tarefas | Tempo ocupado | Tempo ocioso | Utilização |
|---|---|---|---|---|
| 2899 | 25 | **1,2648 s** | 0,0185 s | 98,6% |
| 2898 | 25 | 0,4880 s | **0,7954 s** | 38,0% |
| 2896 | 25 | 0,4855 s | **0,7979 s** | 37,8% |
| 2897 | 25 | 0,4822 s | **0,8011 s** | 37,6% |

### Respostas

**Todos os processos terminam aproximadamente no mesmo momento?**
Depende inteiramente da estratégia e da distribuição dos dados. Na ordem original
(cenário A) sim, quase: diferença de 0,11 s entre o mais e o menos carregado, porque o
acaso distribuiu as 10 tarefas pesadas entre os blocos (2, 3, 2 e 3 pesadas por bloco).
No cenário C, **não**: um processo trabalhou 1,26 s e os outros três, ~0,48 s — os três
ficaram **0,80 s parados esperando** o quarto terminar.

**O balanceamento de carga foi adequado? Justifique.**
No cenário C, não. As 25 tarefas por processo eram iguais **em quantidade**, mas não
**em custo** — o bloco final concentrou todas as tarefas pesadas. Como o tempo total é
sempre o do processo mais lento (makespan), o programa levou 1,2834 s em vez dos 0,77 s
que a mesma máquina consegue quando a carga é bem distribuída: **67% mais lento**, com
2,41 s de tempo de processador simplesmente desperdiçados. Eficiência de 52,1% com
todos os núcleos disponíveis. Nos demais cenários (A, B, D, E) o balanceamento foi
adequado, com eficiência entre 84,9% e 90,5%.

**É possível que alguns processos fiquem ociosos enquanto outros ainda executam?**
Sim — é exatamente o que os números do cenário C mostram: 3 processos com ~38% de
utilização (0,80 s ociosos cada) e um com 98,6%. Somando, foram **2,41 s de capacidade
de processamento jogados fora**, quase o tempo da execução sequencial inteira.

**Como uma estratégia de distribuição dinâmica melhoraria o balanceamento?**
Ela já melhorou, e muito: o cenário D é o mesmo C, mudando apenas `chunksize` de 25 para
1 — o tempo caiu de 1,2834 s para 0,7674 s (**40% mais rápido**), o speedup subiu de 2,08
para 3,49 e a ociosidade caiu de 2,41 s para 0,28 s. A razão: na distribuição dinâmica
nenhum processo recebe trabalho antecipadamente; quem termina antes vai buscar a próxima
tarefa na fila. Assim, quem pega uma tarefa pesada executa menos tarefas leves, e a carga
se equilibra **em tempo de execução**, sem que o programador precise saber de antemão
quais entradas são caras.

**Dividir o trabalho em tarefas menores melhoraria a distribuição? Qual o impacto no
overhead?**
Melhora a distribuição e piora o overhead — os dois efeitos aparecem no cenário E, em que
cada tarefa foi quebrada em 5 subtarefas (500 unidades em vez de 100):

- **Ganho de balanceamento:** o desbalanceamento caiu para **0,0218 s**, o melhor de
  todos os cenários (4,3× melhor que D), com utilização média de 92,3%. Uma tarefa pesada
  deixou de ser um bloco indivisível e passou a poder ser repartida entre processos.
- **Custo de overhead:** 400 despachos a mais × ~263 µs ≈ 0,105 s de trabalho extra, e o
  tempo final ficou levemente **pior** que D (0,7879 s vs. 0,7674 s).

Ou seja: aqui o balanceamento já estava bom o suficiente e refinar a granularidade só
adicionou custo. Se as tarefas pesadas fossem muito maiores (por exemplo, uma única
tarefa correspondendo a 40% do trabalho total), o particionamento passaria a compensar,
porque nenhuma estratégia de fila resolve um bloco indivisível maior que a fatia justa de
cada processo. É a mesma decisão de sempre: **granularidade mais fina = melhor
balanceamento + mais overhead**, e o ponto ótimo fica onde os dois se cruzam.

---

## Análise integrada do ED 01

**Os resultados confirmam que adicionar mais processadores não produz necessariamente
ganho proporcional de desempenho?** Confirmam, e cada conceito do Módulo 01 aparece com
um número medido:

**Speedup.** O speedup cresceu de forma quase linear enquanto havia hardware ocioso
(1,96 com 2 processos; 3,76 com 4) e **parou** quando o hardware acabou (3,79 com 8, isto
é, 47% do ideal). Adicionar 4 processos além dos núcleos existentes não produziu
absolutamente nenhum ganho — a máquina não passa a ter mais capacidade porque o programa
pediu mais processos.

**Eficiência.** A eficiência é a métrica que denuncia o desperdício: 98,0% → 94,0% →
47,4%. O speedup de 3,79 com 8 processos parece bom isoladamente, mas significa que cada
processo entrega menos da metade do que poderia. Em um cluster pago por hora, a segunda
metade dos recursos seria custo puro.

**Overhead.** Ele explica a perda que existe **antes** da saturação e é mensurável:
~10 ms para criar 4 processos e ~263 µs por tarefa distribuída, além de serialização,
IPC, agendamento do SO e reordenação dos resultados. Com 4 processos isso equivale a
0,033 s dos 0,545 s — os 6% que faltaram para a eficiência ideal. Overhead nunca é zero:
todo paralelismo cobra um pedágio, e ele cresce com o número de processos (0,021 s com 2;
0,284 s com 8).

**Granularidade.** Ela determina se esse pedágio é irrelevante ou fatal. Mantendo o
trabalho total constante e apenas fatiando mais fino, o speedup foi de 3,65 (tarefas de
20 ms) para **0,13 (tarefas de 0,04 ms)** — a versão paralela ficou 7,6× mais lenta que a
sequencial. Não existe "paralelizar mais": existe paralelizar na granularidade certa.

**Balanceamento de carga.** Mesmo com hardware sobrando, overhead baixo e granularidade
adequada, uma distribuição ruim derruba o desempenho: no cenário C, 4 núcleos livres
entregaram speedup de apenas 2,08 (eficiência 52,1%) porque três processos ficaram
0,80 s ociosos esperando o quarto. Trocar a distribuição estática por dinâmica — uma
linha de código, `chunksize=1` — recuperou o speedup para 3,49. O tempo de um programa
paralelo é sempre o tempo do **processo mais lento**, nunca a média.

**Lei de Amdahl.** É a moldura que reúne tudo. Como uma fração do programa é
irredutivelmente sequencial (montar a entrada, criar o pool, coletar e ordenar os
resultados), o speedup tem teto: S(N) ≤ 1 / (f + (1−f)/N). Estimando f pela métrica de
Karp–Flatt sobre os dados medidos, obtém-se f ≈ 2,1% até 4 processos — coerente com o
teto observado — e f ≈ 15,9% com 8 processos, quando à parte serial se soma a disputa por
núcleos. É exatamente o que Amdahl prevê: **o ganho é limitado pela parte que não
paraleliza, e essa parte não desaparece com mais processadores**.

A Lei de Gustafson oferece o contraponto observado no Experimento C da Etapa 5: quando o
**tamanho do problema cresce** junto com os recursos (500 tarefas em vez de 20), a fração
paralela aumenta e a eficiência sobe de 75,2% para 96,7% com os mesmos 4 processos. Ou
seja, mais processadores rendem quando há problema grande o bastante para ocupá-los.

**Conclusão.** Desempenho paralelo não é uma função do número de processos, e sim do
equilíbrio entre quatro fatores: recursos físicos realmente disponíveis, overhead de
coordenação, granularidade das tarefas e qualidade da distribuição da carga. Nos
experimentos, o melhor resultado (speedup 3,87, eficiência 96,7%) veio de usar exatamente
o número de processos igual ao de núcleos, com tarefas de ~20 ms e em quantidade
suficiente para manter todos ocupados. Qualquer desvio disso — processos demais, tarefas
pequenas demais ou carga mal distribuída — reduziu o desempenho, em alguns casos até
abaixo do da versão sequencial.

---

## Como reproduzir

```bash
python3 ed01_computacao_paralela.py                 # roda todas as etapas
python3 ed01_computacao_paralela.py --etapa 1       # apenas a etapa 1
python3 ed01_computacao_paralela.py --repeticoes 5  # 5 execuções por medição
python3 medir_overhead.py                           # micro-medições de overhead
```

Ao final, os dados são gravados em `resultados_ed01.csv` e `resultados_ed01.json`.
