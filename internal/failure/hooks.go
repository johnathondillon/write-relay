// Package failure defines inert hook boundaries used by deterministic
// process-crash tests. Production composition always uses the zero value.
package failure

type Hooks struct {
	BeforeSpoolTransaction func()
	AfterSpoolEvent        func(index int)
	AfterSpoolCommit       func()
	AfterAcknowledgment    func()
	BeforeSinkRequest      func()
	AfterSinkSuccess       func()
}

func (h Hooks) CallBeforeSpoolTransaction() {
	if h.BeforeSpoolTransaction != nil {
		h.BeforeSpoolTransaction()
	}
}

func (h Hooks) CallAfterSpoolEvent(index int) {
	if h.AfterSpoolEvent != nil {
		h.AfterSpoolEvent(index)
	}
}

func (h Hooks) CallAfterSpoolCommit() {
	if h.AfterSpoolCommit != nil {
		h.AfterSpoolCommit()
	}
}

func (h Hooks) CallAfterAcknowledgment() {
	if h.AfterAcknowledgment != nil {
		h.AfterAcknowledgment()
	}
}

func (h Hooks) CallBeforeSinkRequest() {
	if h.BeforeSinkRequest != nil {
		h.BeforeSinkRequest()
	}
}

func (h Hooks) CallAfterSinkSuccess() {
	if h.AfterSinkSuccess != nil {
		h.AfterSinkSuccess()
	}
}
