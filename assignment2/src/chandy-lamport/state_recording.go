package chandy_lamport

type StateRecording struct {
	ChannelsStates     map[string]*ChannelState
	InitialState       int
	RecordingCompleted bool
}

type ChannelState struct {
	recording bool
	messages  []*SnapshotMessage
}

func MarkerReceivingRule(
	stateRecording *StateRecording, incomingChannel string, initialState int,
	incomingChannels map[string]*Link) *StateRecording {

	if stateRecording == nil {
		channelsStates := make(map[string]*ChannelState)
		for src := range incomingChannels {
			channelsStates[src] = &ChannelState{src != incomingChannel, nil}
		}
		stateRecording = &StateRecording{
			channelsStates,
			initialState,
			false,
		}
	} else {
		stateRecording.ChannelsStates[incomingChannel].recording = false
	}
	stateRecording.probablyStopStateRecording()
	return stateRecording
}

func (sr *StateRecording) probablyStopStateRecording() {
	channelsRecording := len(sr.ChannelsStates)
	for _, cs := range sr.ChannelsStates {
		if !cs.recording {
			channelsRecording--
		}
	}
	sr.RecordingCompleted = channelsRecording == 0
}

func MarkerSendingRule(initialState int, incomingChannels map[string]*Link) *StateRecording {
	channelsStates := make(map[string]*ChannelState)
	for src := range incomingChannels {
		channelsStates[src] = &ChannelState{true, nil}
	}

	return &StateRecording{
		channelsStates,
		initialState,
		false,
	}
}

func (sr *StateRecording) RecordChannelMessage(src string, dest string, message TokenMessage) {
	if cs := sr.ChannelsStates[src]; cs.recording {
		cs.messages = append(cs.messages, &SnapshotMessage{src, dest, message})
	}
}
