package group

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"sync"
	"time"
)

type Coordinator struct {
	cfg           Conf
	address       string
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
	// WriteOffsets Note that groupEpoch here is different to the generation id which increments on each rebalance
	WriteOffsets(kvs []common.KV, groupID string, groupEpoch int32) error
}

type Conf struct {
	MinSessionTimeout    time.Duration
	MaxSessionTimeout    time.Duration
	InitialJoinDelay     time.Duration
	NewMemberJoinTimeout time.Duration
}

func NewConf() Conf {
	return Conf{
		MinSessionTimeout:    DefaultMinSessionTimeout,
		MaxSessionTimeout:    DefaultMaxSessionTimeout,
		InitialJoinDelay:     DefaultInitialJoinDelay,
		NewMemberJoinTimeout: DefaultNewMemberJoinTimeout,
	}
}

const (
	DefaultMinSessionTimeout    = 6 * time.Second
	DefaultMaxSessionTimeout    = 30 * time.Minute
	DefaultInitialJoinDelay     = 3 * time.Second
	DefaultNewMemberJoinTimeout = 5 * time.Minute
)

func NewCoordinator(cfg Conf, address string, topicProvider topicInfoProvider, controlClientCache *control.ClientCache,
	pusherClient tablePusherClient, tableGetter sst.TableGetter) (*Coordinator, error) {
	return &Coordinator{
		cfg:           cfg,
		address:       address,
		groups:        map[string]*group{},
		topicProvider: topicProvider,
		clientCache:   controlClientCache,
		pusherClient:  pusherClient,
		tableGetter:   tableGetter,
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

// TODO Should be called when if agent leaves cluster
func (gc *Coordinator) clearGroups() {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	gc.groups = map[string]*group{}
}

func (gc *Coordinator) FindCoordinator(groupID string) (string, error) {
	cl, err := gc.clientCache.GetClient()
	if err != nil {
		return "", err
	}
	clusterMemberID, _, err := cl.GetGroupCoordinatorInfo(groupID)
	return clusterMemberID, err
}

func (gc *Coordinator) JoinGroup(apiVersion int16, groupID string, clientID string, memberID string, protocolType string,
	protocols []ProtocolInfo, sessionTimeout time.Duration, reBalanceTimeout time.Duration, completionFunc JoinCompletion) {
	if sessionTimeout < gc.cfg.MinSessionTimeout || sessionTimeout > gc.cfg.MaxSessionTimeout {
		gc.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeInvalidSessionTimeout)
		return
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		cl, err := gc.clientCache.GetClient()
		if err != nil {
			log.Warnf("failed to get controller client to get coordinator info: %v", err)
			gc.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeLeaderNotAvailable)
			return
		}
		address, groupEpoch, err := cl.GetGroupCoordinatorInfo(groupID)
		if err != nil {
			log.Warnf("failed to get coordinator info: %v", err)
			gc.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeLeaderNotAvailable)
			return
		}
		if address != gc.address {
			gc.sendJoinError(completionFunc, kafkaprotocol.ErrorCodeNotCoordinator)
			return
		}
		g = gc.createGroup(groupID, groupEpoch)
	}
	g.Join(apiVersion, clientID, memberID, protocolType, protocols, sessionTimeout, reBalanceTimeout, completionFunc)
}

func (gc *Coordinator) SyncGroup(groupID string, memberID string, generationID int, assignments []AssignmentInfo,
	completionFunc SyncCompletion) {
	if memberID == "" {
		gc.sendSyncError(completionFunc, kafkaprotocol.ErrorCodeUnknownMemberID)
		return
	}
	g, ok := gc.getGroup(groupID)
	if !ok {
		gc.sendSyncError(completionFunc, kafkaprotocol.ErrorCodeGroupIDNotFound)
		return
	}
	g.Sync(memberID, generationID, assignments, completionFunc)
}

func (gc *Coordinator) HeartbeatGroup(groupID string, memberID string, generationID int) int {
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

func (gc *Coordinator) sendJoinError(completionFunc JoinCompletion, errorCode int) {
	completionFunc(JoinResult{ErrorCode: errorCode})
}

func (gc *Coordinator) sendSyncError(completionFunc SyncCompletion, errorCode int) {
	completionFunc(errorCode, nil)
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

func (gc *Coordinator) createGroup(groupID string, groupEpoch int32) *group {
	gc.groupsLock.Lock()
	defer gc.groupsLock.Unlock()
	g, ok := gc.groups[groupID]
	if ok {
		return g
	}
	partHash, err := parthash.CreateHash([]byte(groupID))
	if err != nil {
		panic(err) // doesn't happen
	}
	g = &group{
		gc:                      gc,
		id:                      groupID,
		groupEpoch:              groupEpoch,
		partHash:                partHash,
		state:                   stateEmpty,
		members:                 map[string]*member{},
		pendingMemberIDs:        map[string]struct{}{},
		supportedProtocolCounts: map[string]int{},
		committedOffsets:        map[int]map[int32]int64{},
	}
	gc.groups[groupID] = g
	return g
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
	statePreReBalance      = 1
	stateAwaitingReBalance = 2
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
