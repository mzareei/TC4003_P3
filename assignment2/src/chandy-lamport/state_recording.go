package chandy_lamport

type StateRecording struct {
	ChannelsStates     map[string]*ChannelState
	InitialState       int
	RecordingCompleted bool
	recordingChannels  int
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
			len(incomingChannels)-1,
		}
	} else {
		stateRecording.ChannelsStates[incomingChannel].recording = false
		stateRecording.recordingChannels--
	}
	stateRecording.RecordingCompleted = stateRecording.recordingChannels == 0
	return stateRecording
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
		len(incomingChannels),
	}
}

func (sr *StateRecording) RecordChannelMessage(src string, dest string, message TokenMessage) {
	if cs := sr.ChannelsStates[src]; cs.recording {
		cs.messages = append(cs.messages, &SnapshotMessage{src, dest, message})
	}
}
