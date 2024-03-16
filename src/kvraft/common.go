package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDefault     = ""
	ErrRepTimeout  = "ErrRepTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// clerk's ID that issued this request, and request ID of this clerk
	ClerkID   int64
	RequestID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// clerk's ID that issued this request, and request ID of this clerk
	ClerkID   int64
	RequestID int64
}

type GetReply struct {
	Err   Err
	Value string
}
