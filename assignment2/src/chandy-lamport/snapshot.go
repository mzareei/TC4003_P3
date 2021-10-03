package chandy_lamport

type ServerSnapshot struct {
	recordingLinks map[string]bool // key = src
	initialState   int
	Messages       []*SnapshotMessage
	Stop           bool
}

func NewOriginServerSnapshot(initialState int, inLinks map[string]*Link) *ServerSnapshot {
	links := make(map[string]bool)
	for _, l := range inLinks {
		links[l.src] = true
	}
	return &ServerSnapshot{
		links,
		initialState,
		[]*SnapshotMessage{},
		false,
	}
}

func NewServerSnapshot(initialState int, inLinks map[string]*Link, inLink string) *ServerSnapshot {
	links := make(map[string]bool)
	for _, l := range inLinks {
		links[l.src] = l.src != inLink
	}
	return &ServerSnapshot{
		links,
		initialState,
		[]*SnapshotMessage{},
		false,
	}
}

func (snap *ServerSnapshot) StopRecordingLink(link string) {
	if snap.recordingLinks[link] {
		snap.recordingLinks[link] = false
		snap.possiblyStopSnapshot()
	}
}

func (snap *ServerSnapshot) possiblyStopSnapshot() {
	linksRecording := len(snap.recordingLinks)
	for _, recording := range snap.recordingLinks {
		if !recording {
			linksRecording--
		}
	}
	if linksRecording == 0 {
		snap.Stop = true
	}
}

func (snap *ServerSnapshot) NewSnapshotMessage(src string, dest string, message TokenMessage) {
	if snap.recordingLinks[src] {
		snap.Messages = append(snap.Messages, &SnapshotMessage{src, dest, message})
	}
}

func (snap *ServerSnapshot) GetState() int {
	return snap.initialState
}
