package chandy_lamport

// Used by servers to record their complete state for a given snapshot.
// When RecordingCompleted is set, InitialState and the entries of ChannelStates
// contain the complete state of the related server.
type StateRecording struct {
	ChannelsStates     map[string]*ChannelState
	InitialState       int
	RecordingCompleted bool
	recordingChannels  int
}

// Used to keep the state of a channel. 
// While recording is set, incoming messages from the related channel are stored.
type ChannelState struct {
	recording bool
	messages  []*SnapshotMessage
}


// Directly from CL's paper: 
// On receiving a marker along a channel C:
//	if q has not recorded its state then
//		begin q records its state;
//		q records the state c as the empty sequence
//	end
//	else q records the state of c as the sequence of messages 
//		received along c after q’s state
// 		was recorded and before q received the marker along c. 
// Also the possibility of reaching a complete recording is evaluated here.
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

// Directly from CL's paper:
// For each channel c, incident on, and directed away from p:
// 		p sends one marker along c after p records its state and before p sends further messages
// 		along c. 
// The sending marker part is done in the server. 
// Here we only initialize ChannelStates so that all messages from incoming channels are recorded.
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

// Called to record a channel from an incoming channel on the related server.
func (sr *StateRecording) RecordChannelMessage(src string, dest string, message TokenMessage) {
	if cs := sr.ChannelsStates[src]; cs.recording {
		cs.messages = append(cs.messages, &SnapshotMessage{src, dest, message})
	}
}
