package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ----------------------------
// Tipus bàsics de Raft
// ----------------------------

type MsgType int

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteResp
	MsgAppendEntries     // heartbeat
	MsgAppendEntriesResp // no l'usem gaire en aquesta versió simple
)

type Message struct {
	From        int
	To          int
	Term        int
	Type        MsgType
	VoteGranted bool // només s'usa en MsgRequestVoteResp
}

// Estat del node
type State int

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ----------------------------
// Transport local (in-memory)
// ----------------------------

// Transport molt simple basat en canals: no hi ha pèrdues ni retards.
type Transport interface {
	Send(m Message)
	Recv(id int) <-chan Message
}

// LocalTransport guarda un canal per cada node id.
type LocalTransport struct {
	inbox map[int]chan Message
}

func NewLocalTransport(numNodes int) *LocalTransport {
	t := &LocalTransport{
		inbox: make(map[int]chan Message, numNodes),
	}
	for i := 0; i < numNodes; i++ {
		t.inbox[i] = make(chan Message, 64)
	}
	return t
}

func (t *LocalTransport) Send(m Message) {
	ch, ok := t.inbox[m.To]
	if !ok {
		// En un entorn real podríem loguejar l'error.
		return
	}
	ch <- m
}

func (t *LocalTransport) Recv(id int) <-chan Message {
	return t.inbox[id]
}

// ----------------------------
// Node de Raft
// ----------------------------

type Node struct {
	id        int
	peers     []int // llista dels altres nodes, sense contarse ell
	transport Transport

	// estat persistent (simplificat)
	term int

	// Per qui voto
	votedFor int

	// només per debug
	printMu sync.Mutex
}

func NewNode(id int, peers []int, tr Transport) *Node {
	return &Node{
		id:        id,
		peers:     peers,
		transport: tr,
		term:      0,
		votedFor:  -1,
	}
}

// log segur per evitar barreja de línies
func (n *Node) logf(format string, args ...any) {
	n.printMu.Lock()
	defer n.printMu.Unlock()
	prefix := fmt.Sprintf("[n%d t=%d] ", n.id, n.term)
	fmt.Printf(prefix+format+"\n", args...)
}

// ----------------------------
// ACTIVITAT 1: Funcions bàsiques
// ----------------------------

// Timeout d’elecció aleatori (p. ex. 300–600 ms).
func randomElectionTimeout() time.Duration {
	// [cite: 59] Interval entre 300 y 600 ms
	min := 300
	max := 600
	r := rand.Intn(max-min) + min
	return time.Duration(r) * time.Millisecond
}

// Envia un missatge a tots els altres nodes.
func (n *Node) broadcastHeartbeat() {
	// [cite: 70] Enviar MsgAppendEntries a tots els peers
	for _, peer := range n.peers {
		msg := Message{
			From: n.id,
			To:   peer,
			Term: n.term,
			Type: MsgAppendEntries,
		}
		n.transport.Send(msg)
	}
}

// Handler de RequestVote en estat follower.
// Retorna true si s'ha concedit el vot.
func (n *Node) handleRequestVoteFollower(m Message) bool {
	// [cite: 80-86] Gestió del vot
	// 1. Term check
	if m.Term < n.term {
		// Term antic, rebutgem i responem amb el nostre term actualitzat
		// (La resposta s'envia al final)
	} else if m.Term > n.term {
		// Si el term és millor, actualitzem
		n.term = m.Term
		n.votedFor = -1
	}

	voteGranted := false
	// Concedim vot si:
	// a) El term del candidat és com a mínim el nostre (ja verificat/actualitzat a dalt)
	// b) No hem votat (votedFor == -1) o ja hem votat a aquest candidat
	if m.Term == n.term && (n.votedFor == -1 || n.votedFor == m.From) {
		voteGranted = true
		n.votedFor = m.From
	}

	// 3. Envia MsgRequestVoteResp
	resp := Message{
		From:        n.id,
		To:          m.From,
		Term:        n.term,
		Type:        MsgRequestVoteResp,
		VoteGranted: voteGranted,
	}
	n.transport.Send(resp)

	return voteGranted
}

// GESTIO DEL MISSATGE DEL LIDER
// Handler de AppendEntries (heartbeat) en estat follower.
// Retorna true si s'ha acceptat el terme del lider.
func (n *Node) handleAppendEntriesFollower(m Message) bool {
	// [cite: 95-97] Gestió del heartbeat
	success := false

	// 1. Comprovar term
	if m.Term < n.term {
		// Ignorar liders antics
		success = false
	} else {
		// Term vàlid o superior
		if m.Term > n.term {
			n.term = m.Term
			n.votedFor = -1
		}
		// Acceptem el líder
		success = true
	}

	// 2. Enviar resposta
	resp := Message{
		From: n.id,
		To:   m.From,
		Term: n.term,
		Type: MsgAppendEntriesResp,
		// En Raft real aquí iría Success: success, pero en esta versión simple solo confirmamos recepción
	}
	n.transport.Send(resp)

	return success
}

// ----------------------------
// Bucle principal del node
// ----------------------------

// Run arrenca el bucle principal del node, canviant entre estats.
func (n *Node) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	state := StateFollower
	n.logf("arrenca com %s", state)

	for {
		if ctx.Err() != nil {
			n.logf("aturant node")
			return
		}
		switch state {
		case StateFollower:
			state = n.runFollower(ctx)
		case StateCandidate:
			state = n.runCandidate(ctx)
		case StateLeader:
			state = n.runLeader(ctx)
		default:
			n.logf("estat desconegut, sortint")
			return
		}
	}
}

// Estat follower: espera heartbeats o peticions de vot.
// Si passa el timeout sense res, es converteix en candidate.
func (n *Node) runFollower(ctx context.Context) State {
	n.logf("→ Follower")

	// Timer inicial
	timeout := time.After(randomElectionTimeout())

	for {
		select {
		case <-ctx.Done():
			return StateFollower
		case <-timeout:
			n.logf("timeout d'elecció, passant a Candidate")
			return StateCandidate
		case m := <-n.transport.Recv(n.id):
			// Sempre actualitzem term si veiem un més gran
			if m.Term > n.term {
				n.term = m.Term
				n.votedFor = -1
			}

			switch m.Type {
			case MsgAppendEntries: // rebo el heartbeat del leader
				if ok := n.handleAppendEntriesFollower(m); ok {
					// Si acceptem heartbeat, resetegem timer
					timeout = time.After(randomElectionTimeout())
				}
			case MsgRequestVote:
				if n.handleRequestVoteFollower(m) {
					n.logf("votat a %d al term %d", m.From, n.term)
					// Opcional: en algunes variants de Raft votar també reseteja el timeout,
					// però estrictament el follower timeout és per heartbeats.
					// Aquí ho deixem córrer.
				}
			case MsgRequestVoteResp, MsgAppendEntriesResp:
				// En estat follower, ignorem respostes.
			}
		}
	}
}

// ----------------------------
// ACTIVITAT 2: Estat Candidate
// ----------------------------

// Estat candidate:
// - Incrementa term.
// - Es vota a si mateix
// - Envia RequestVote a tots els peers.
// - Espera respostes fins majoria o fins rebre un AppendEntries d’un leader.
func (n *Node) runCandidate(ctx context.Context) State {
	n.logf("→ Candidate")

	// 1. Incrementar term i votar-se a si mateix [cite: 126-128]
	n.term++
	n.votedFor = n.id
	votesReceived := 1 // Vot propi

	// 2. Enviar RequestVote a tots els peers [cite: 129]
	for _, peer := range n.peers {
		msg := Message{
			From: n.id,
			To:   peer,
			Term: n.term,
			Type: MsgRequestVote,
		}
		n.transport.Send(msg)
	}

	// 3. Timeout d'elecció propi per aquesta ronda [cite: 130]
	timeout := time.After(randomElectionTimeout())

	for {
		select {
		case <-ctx.Done():
			return StateFollower

		case <-timeout:
			// [cite: 136] Sense resultat -> reintentar (nova ronda de Candidate)
			n.logf("elecció sense resultat, reintentant")
			return StateCandidate

		case m := <-n.transport.Recv(n.id):
			// Si trobem un term més alt, baixem a follower immediatament
			if m.Term > n.term {
				n.term = m.Term
				n.votedFor = -1
				n.logf("He vist term més gran (%d) mentre era candidate -> Follower", m.Term)

				// Si era un RequestVote, li responem abans de baixar (cortesia)
				if m.Type == MsgRequestVote {
					n.handleRequestVoteFollower(m)
				}
				return StateFollower
			}

			switch m.Type {
			case MsgRequestVoteResp:
				// [cite: 131-133] Gestionar respostes de vot
				if m.Term == n.term && m.VoteGranted {
					votesReceived++
					// Càlcul de majoria: (N peers + jo) / 2 + 1
					nodesTotal := len(n.peers) + 1
					majority := (nodesTotal / 2) + 1

					if votesReceived >= majority {
						n.logf("Majoria aconseguida (%d/%d), esdevenint Leader", votesReceived, nodesTotal)
						return StateLeader
					}
				}

			case MsgAppendEntries:
				// [cite: 135] Si arriba un heartbeat valid (term >= meu), hi ha un altre líder
				if m.Term >= n.term {
					n.logf("Rebut heartbeat de líder vàlid -> Follower")
					// Processar el missatge per respondre-li
					n.handleAppendEntriesFollower(m)
					return StateFollower
				}

			case MsgRequestVote:
				// Podem rebre peticions d'altres candidats.
				// Si tenen el mateix term, ja he votat per mi (votedFor=jo), així que denegaré.
				// Si tenen term menor, denego.
				// Si tenen term major, ja hauria sortit pel check de dalt.
				n.handleRequestVoteFollower(m)
			}
		}
	}
}

// ----------------------------
// ACTIVITAT 3: Estat Leader
// ----------------------------

// Estat leader: envia heartbeats periòdics als peers.
// Si rep un RequestVote o AppendEntries amb term més gran, torna a follower.
func (n *Node) runLeader(ctx context.Context) State {
	n.logf("→ Leader")

	// [cite: 162] Ticker per heartbeats (100-150 ms)
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	// Enviar heartbeat inicial per consolidar autoritat
	n.broadcastHeartbeat()

	for {
		select {
		case <-ctx.Done():
			return StateFollower

		case <-ticker.C:
			// [cite: 163] Enviar heartbeats periòdics
			n.broadcastHeartbeat()

		case m := <-n.transport.Recv(n.id):
			// [cite: 164-165] Comprovar term superior
			if m.Term > n.term {
				n.term = m.Term
				n.votedFor = -1
				n.logf("he vist term més alt (%d), baixant a follower", m.Term)

				// Gestionar missatge entrant com a follower per no perdre lògica
				if m.Type == MsgRequestVote {
					n.handleRequestVoteFollower(m)
				} else if m.Type == MsgAppendEntries {
					n.handleAppendEntriesFollower(m)
				}
				return StateFollower
			}

			switch m.Type {
			case MsgRequestVote:
				// Si el term no és superior, deneguem el vot (ja som líder)
				n.handleRequestVoteFollower(m)

			case MsgAppendEntries:
				// Si rebo un AppendEntries amb el MATEIX term que jo,
				// vol dir que hi ha dos líders (split brain resolt per qui té term més alt).
				// Si m.Term == n.term, per seguretat torno a follower i que guanyi el millor a la reelecció.
				if m.Term == n.term {
					n.logf("Conflicte de líders al mateix term -> Follower")
					return StateFollower
				}

			case MsgRequestVoteResp, MsgAppendEntriesResp:
				// Per aquesta versió simple, ignorem respostes.
			}
		}
	}
}

func main() {
	// Inicializar la semilla para números aleatorios
	rand.Seed(time.Now().UnixNano())

	// Número de nodos para el experimento (puedes cambiarlo a 5 o 7) [cite: 22, 170]
	const numNodes = 21

	// Transport compartido [cite: 23]
	tr := NewLocalTransport(numNodes)

	// Crear nodos [cite: 24]
	nodes := make([]*Node, 0, numNodes)
	for id := 0; id < numNodes; id++ {
		var peers []int
		for j := 0; j < numNodes; j++ {
			if j != id {
				peers = append(peers, j)
			}
		}
		nodes = append(nodes, NewNode(id, peers, tr))
	}

	// Contexto para detener todos los nodos de forma coordinada [cite: 25]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(numNodes)
	for _, n := range nodes {
		// Lanzar cada node en una goroutine [cite: 26]
		go n.Run(ctx, &wg)
	}

	// Dejar correr el clúster (por ejemplo, 10 segundos) [cite: 27, 171]
	time.Sleep(10 * time.Second)

	// Aturar el cluster
	n := "\n--- DETENIENDO CLUSTER ---"
	fmt.Println(n)
	cancel()
	wg.Wait()
}
