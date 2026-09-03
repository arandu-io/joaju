package joaju

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// sendTestSink is a sink with a queue and no writer goroutine.
//
// The writer is left out on purpose. What these tests ask about is the answer
// Send gives, and a goroutine draining the queue underneath would decide that
// answer as much as Send does -- a full queue would empty on its own timing and
// a closed socket would be racing a shutdown. Nothing here touches the socket:
// Send and Terminate never do.
func sendTestSink(queue int) *sink {
	return &sink{
		out:          make(chan []byte, queue),
		done:         make(chan struct{}),
		writeTimeout: DefaultWriteTimeout,
		pingInterval: DefaultPingInterval,
	}
}

// closedSendAttempts is how many frames the closed socket is offered.
//
// One would prove the contract, since the answer has to hold for every call.
// The repetition is what makes this a guard rather than a coin toss: the defect
// it fixes let a closed socket accept a frame about half the time, so a single
// call would notice a regression only half the time it happened.
const closedSendAttempts = 16

func TestSendRefusesEveryFrameOnASocketAlreadyClosed(t *testing.T) {
	k := sendTestSink(DefaultOutboundQueue)

	if err := k.Terminate(context.Background()); err != nil {
		t.Fatalf("Terminate() = %v, want nil", err)
	}

	for attempt := range closedSendAttempts {
		err := k.Send(context.Background(), []byte("after the close"))
		if !errors.Is(err, ErrSocketClosed) {
			t.Fatalf("Send() answered %v on attempt %d, want %v",
				err, attempt, ErrSocketClosed)
		}
	}

	if queued := len(k.out); queued != 0 {
		t.Fatalf("the closed socket queued %d frames, want 0", queued)
	}
}

func TestSendQueuesAFrameWhileTheSocketIsOpen(t *testing.T) {
	k := sendTestSink(2)

	if err := k.Send(context.Background(), []byte("first")); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}

	if queued := len(k.out); queued != 1 {
		t.Fatalf("the open socket queued %d frames, want 1", queued)
	}

	select {
	case <-k.done:
		t.Fatal("Send() closed a socket that had room for the frame")
	default:
	}
}

func TestSendClosesTheSocketThatFellBehind(t *testing.T) {
	k := sendTestSink(2)

	for range cap(k.out) {
		if err := k.Send(context.Background(), []byte("queued")); err != nil {
			t.Fatalf("Send() = %v, want nil while there was room", err)
		}
	}

	err := k.Send(context.Background(), []byte("one too many"))
	if !errors.Is(err, ErrSocketClosed) {
		t.Fatalf("Send() = %v, want %v", err, ErrSocketClosed)
	}
	if !strings.Contains(err.Error(), "fell behind") {
		t.Fatalf("Send() = %q, want it to say the client fell behind", err)
	}

	select {
	case <-k.done:
	default:
		t.Fatal("Send() left open a socket it had refused a frame for")
	}
}

// TestSendIsSafeForConcurrentCallersOnAnOpenSocket holds the normal path, which
// is the one a broadcast takes: every subscriber of a channel queues on its own
// goroutine, and none of them may be refused while the socket is open and the
// queue has room.
func TestSendIsSafeForConcurrentCallersOnAnOpenSocket(t *testing.T) {
	const callers = 32

	k := sendTestSink(callers)

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errs   []error
		ctx    = context.Background()
		frame  = []byte("broadcast")
		refuse = func(err error) {
			mu.Lock()
			defer mu.Unlock()

			errs = append(errs, err)
		}
	)

	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()

			if err := k.Send(ctx, frame); err != nil {
				refuse(err)
			}
		}()
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("Send() refused %d of %d frames on an open socket: %v",
			len(errs), callers, errs[0])
	}
	if queued := len(k.out); queued != callers {
		t.Fatalf("the open socket queued %d frames, want %d", queued, callers)
	}
}
