package group

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"sync"
	"time"
)

type Coordinator struct {
	cfg           *conf.Config
	topicProvider topicInfoProvider
	clientCache   *control.ClientCache
	pusherClient  tablePusherClient
	tableGetter   sst.TableGetter
	groups        map[string]*group
	groupsLock    sync.RWMutex
	timers        sync.Map
}

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, error)
}

type tablePusherClient interface {
	WriteKVs(kvs []common.KV) error
}

// FIXME create config struct
const maxControllerClients = 10

func NewCoordinator(cfg *conf.Config, topicProvider topicInfoProvider,
	controlClientFactory control.ClientFactory, pusherClient tablePusherClient, tableGetter sst.TableGetter) (*Coordinator, error) {
	return &Coordinator{
		cfg:           cfg,
		groups:        map[string]*group{},
		topicProvider: topicProvider,
		// FIXME - pass in client cache from agent and share with fetcher?
		clientCache:  control.NewClientCache(maxControllerClients, controlClientFactory),
		pusherClient: pusherClient,
		tableGetter:  tableGetter,
	}, nil
}

func (gc *Coordinator) Start() error {
	return nil
}

func (gc *Coordinator) Stop() error {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	for _, g := range gc.groups {
		g.stop()
	}
	return nil
}

func (gc *Coordinator) FindCoordinator(groupID string) int {
	// TODO - contact the controller to get the agent address for the group - the controller will maintain a mapping
	// and mapping of group to agent will be preserved as long as agent is alive - we can't have mapping changing
	// if we add / remove other agents to the cluster as that would cause consumers to have to rebalance.
	return -1
}

func (gc *Coordinator) JoinGroup(apiVersion int16, groupID string, clientID string, memberID string, protocolType string,
	protocols []ProtocolInfo, sessionTimeout time.Duration, rebalanceTimeout time.Duration, complFunc JoinCompletion) {
	if !gc.checkLeader(groupID) {
		gc.sendJoinError(complFunc, kafkaprotocol.ErrorCodeNotCoordinator)
		return
	}
	if sessionTimeout < gc.cfg.KafkaMinSessionTimeout || sessionTimeout > gc.cfg.KafkaMaxSessionTimeout {
		gc.sendJoinError(complFunc, kafkaprotocol.ErrorCodeInvalidSessionTimeout)
		return
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		var err error
		g, err = gc.createGroup(groupID)
		if err != nil {
			// FIXME??????????????? handle error
		}
	}
	g.Join(apiVersion, clientID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout, complFunc)
}

func (gc *Coordinator) SyncGroup(groupID string, memberID string, generationID int, assignments []AssignmentInfo,
	complFunc SyncCompletion) {
	if !gc.checkLeader(groupID) {
		gc.sendSyncError(complFunc, kafkaprotocol.ErrorCodeNotCoordinator)
		return
	}
	if memberID == "" {
		gc.sendSyncError(complFunc, kafkaprotocol.ErrorCodeUnknownMemberID)
		return
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		gc.sendSyncError(complFunc, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return
	}
	g.Sync(memberID, generationID, assignments, complFunc)
}

func (gc *Coordinator) HeartbeatGroup(groupID string, memberID string, generationID int) int {
	if !gc.checkLeader(groupID) {
		return kafkaprotocol.ErrorCodeNotCoordinator
	}
	if memberID == "" {
		return kafkaprotocol.ErrorCodeUnknownMemberID
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		return kafkaprotocol.ErrorCodeGroupIDNotFound
	}
	return g.Heartbeat(memberID, generationID)
}

func (gc *Coordinator) LeaveGroup(groupID string, leaveInfos []MemberLeaveInfo) int16 {
	if !gc.checkLeader(groupID) {
		return kafkaprotocol.ErrorCodeNotCoordinator
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		return kafkaprotocol.ErrorCodeGroupIDNotFound
	}
	return g.Leave(leaveInfos)
}

func (gc *Coordinator) OffsetCommit(req *kafkaprotocol.OffsetCommitRequest) *kafkaprotocol.OffsetCommitResponse {
	var resp kafkaprotocol.OffsetCommitResponse
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicData.Partitions))
	}
	groupID := *req.GroupId
	if !gc.checkLeader(groupID) {
		fillAllErrorCodesForOffsetCommit(&resp, kafkaprotocol.ErrorCodeNotCoordinator)
		return &resp
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		fillAllErrorCodesForOffsetCommit(&resp, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return &resp
	}
	g.offsetCommit(req, &resp)
	return &resp
}

func (gc *Coordinator) OffsetFetch(req *kafkaprotocol.OffsetFetchRequest) *kafkaprotocol.OffsetFetchResponse {
	var resp kafkaprotocol.OffsetFetchResponse
	resp.Topics = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponsePartition, len(topicData.PartitionIndexes))
	}
	groupID := *req.GroupId
	if !gc.checkLeader(groupID) {
		fillAllErrorCodesForOffsetFetch(&resp, kafkaprotocol.ErrorCodeNotCoordinator)
		return &resp
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		fillAllErrorCodesForOffsetFetch(&resp, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return &resp
	}
	g.offsetFetch(req, &resp)
	return &resp
}

func (gc *Coordinator) getGroup(groupID string) (*group, bool) {
	gc.groupsLock.RLock()
	defer gc.groupsLock.RUnlock()
	g, ok := gc.groups[groupID]
	return g, ok
}

func (gc *Coordinator) checkLeader(groupID string) bool {
	leaderNode := gc.FindCoordinator(groupID)
	return leaderNode == gc.cfg.NodeID
}

func (gc *Coordinator) sendJoinError(complFunc JoinCompletion, errorCode int) {
	complFunc(JoinResult{ErrorCode: errorCode})
}

func (gc *Coordinator) sendSyncError(complFunc SyncCompletion, errorCode int) {
	complFunc(errorCode, nil)
}

func (gc *Coordinator) getState(groupID string) int {
	gc.groupsLock.RLock()
	defer gc.groupsLock.RUnlock()
	group, ok := gc.groups[groupID]
	if !ok {
		return -1
	}
	return group.getState()
}

func (gc *Coordinator) groupHasMember(groupID string, memberID string) bool {
	g, ok := gc.getGroup(groupID)
	if !ok {
		return false
	}
	return g.hasMember(memberID)
}

func (gc *Coordinator) createGroup(groupID string) (*group, error) {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	g, ok := gc.groups[groupID]
	if ok {
		return g, nil
	}
	partHash, err := parthash.CreateHash([]byte(groupID))
	if err != nil {
		return nil, err
	}
	g = &group{
		gc:                      gc,
		id:                      groupID,
		partHash:                partHash,
		state:                   stateEmpty,
		members:                 map[string]*member{},
		pendingMemberIDs:        map[string]struct{}{},
		supportedProtocolCounts: map[string]int{},
		committedOffsets:        map[int]map[int32]int64{},
	}
	gc.groups[groupID] = g
	return g, nil
}

func (gc *Coordinator) setTimer(timerKey string, delay time.Duration, action func()) {
	timer := common.ScheduleTimer(delay, false, action)
	gc.timers.Store(timerKey, timer)
}

func (gc *Coordinator) cancelTimer(timerKey string) {
	t, ok := gc.timers.Load(timerKey)
	if !ok {
		return
	}
	t.(*common.TimerHandle).Stop()
}

func (gc *Coordinator) rescheduleTimer(timerKey string, delay time.Duration, action func()) {
	gc.cancelTimer(timerKey)
	gc.setTimer(timerKey, delay, action)
}

const (
	stateEmpty             = 0
	statePreRebalance      = 1
	stateAwaitingRebalance = 2
	stateActive            = 3
	stateDead              = 4
)

type MemberInfo struct {
	MemberID string
	MetaData []byte
}

type ProtocolInfo struct {
	Name     string
	Metadata []byte
}

type MemberLeaveInfo struct {
	MemberID        string
	GroupInstanceID *string
}

type JoinCompletion func(result JoinResult)

type JoinResult struct {
	ErrorCode      int
	MemberID       string
	LeaderMemberID string
	ProtocolName   string
	GenerationID   int
	Members        []MemberInfo
}

type SyncCompletion func(errorCode int, assignment []byte)

type HeartbeatCompletion func(errorCode int)

type AssignmentInfo struct {
	MemberID   string
	Assignment []byte
}
