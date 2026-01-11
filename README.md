# RaftGo

Implementación del algoritmo de consenso **Raft** en Go: elección automática de líder,
términos para evitar el *split brain* y heartbeats para mantener la autoridad. Un solo
fichero, sin dependencias externas, con los nodos comunicándose por canales de Go.

Enero de 2026.

## El algoritmo, en lo que está implementado

Cada nodo guarda su identificador, la lista de iguales y su transporte, y vive en uno de
tres estados:

- **Follower**: espera mensajes. Si se le agota el temporizador de elección, pasa a candidato.
- **Candidate**: incrementa el término, se vota a sí mismo y pide el voto a todos los demás
  con `MsgRequestVote`.
- **Leader**: al reunir la mayoría (`N/2 + 1`) asume el mando y manda heartbeats cada
  **150 ms** para que nadie convoque elecciones nuevas.

La pieza que evita el empate perpetuo es `randomElectionTimeout`, que devuelve una duración
aleatoria **entre 300 y 600 ms**: si todos los nodos tuvieran el mismo temporizador, todos se
harían candidatos a la vez y la elección no terminaría nunca.

## Experimentos

Con 3, 5 y 7 nodos, el clúster elige líder siempre en el primer término:

| Nodos | Mayoría necesaria | Término final | Tiempo hasta líder |
|---|---|---|---|
| 3 | 2 | t=1 | 0,45 s |
| 5 | 3 | t=1 | 0,75 s |
| 7 | 4 | t=1 | 0,82 s |

Y variando el temporizador se ve el compromiso de frente:

- **Conservador (500–900 ms)**: tarda más en elegir, 1,15 s.
- **Agresivo (200–400 ms)**: elige antes, pero aumenta el riesgo de reelecciones continuas.

Se probó también con 21 nodos, donde se observa cómo la gestión de términos resuelve las
reelecciones.

## Ejecutarlo

```bash
go run .
```

Requiere Go 1.22 o superior.

## Autoría

Autoría: mmoreno | avidosa
