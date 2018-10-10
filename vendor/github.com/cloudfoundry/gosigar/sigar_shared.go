package sigar

import "time"

func (self *ProcCpu) Get(pid int) error {
	if self.cache == nil {
		self.cache = make(map[int]ProcCpu)
	}
	prevProcCpu := self.cache[pid]

	procTime := &ProcTime{}
	if err := procTime.Get(pid); err != nil {
		return err
	}
	self.StartTime = procTime.StartTime
	self.User = procTime.User
	self.Sys = procTime.Sys
	self.Total = procTime.Total

	self.LastTime = uint64(time.Now().UnixNano() / int64(time.Millisecond))
	self.cache[pid] = *self

	if prevProcCpu.LastTime == 0 {
		time.Sleep(100 * time.Millisecond)
		return self.Get(pid)
	}

	self.Percent = float64(self.Total-prevProcCpu.Total) / float64(self.LastTime-prevProcCpu.LastTime)
	return nil
}
