package controlplane

import (
	"errors"
	"net/http"
	"strings"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

const recentAttempts = 20

func (s *Server) badRequest(w http.ResponseWriter, r *http.Request, err error) {
	if fe, ok := asFieldError(err); ok && fe.isUnknownType {
		writeError(w, r, s.log, http.StatusBadRequest, codeUnknownJobType, fe.msg)
		return
	}
	writeError(w, r, s.log, http.StatusBadRequest, codeInvalidRequest, err.Error())
}

func (s *Server) newID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.NewV7()
	if err != nil {
		s.writeStoreError(w, r, err)
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decodeJSON(r, &req, false); err != nil {
		s.badRequest(w, r, err)
		return
	}
	v, err := validateSubmit(req, s.jobTypes)
	if err != nil {
		s.badRequest(w, r, err)
		return
	}

	nj := store.NewJob{Type: v.Type, Payload: v.Payload, Priority: v.Priority, MaxRetries: v.MaxRetries,
		TimeoutSeconds: v.TimeoutSeconds, RunAt: v.RunAt}
	if _, present := r.Header["Idempotency-Key"]; present {
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if key == "" {
			s.badRequest(w, r, invalid("Idempotency-Key must not be empty"))
			return
		}
		if err := validateIdempotencyKey(key); err != nil {
			s.badRequest(w, r, err)
			return
		}
		hash, err := requestHash(v)
		if err != nil {
			s.writeStoreError(w, r, err)
			return
		}
		nj.IdempotencyKey, nj.RequestHash = &key, &hash
	}
	id, ok := s.newID(w, r)
	if !ok {
		return
	}
	nj.ID = id

	res, err := s.st.EnqueueJob(r.Context(), nj)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	body := submitResponse{JobID: res.Job.ID, Status: res.Job.Status, RunAt: res.Job.RunAt}
	if !res.Created {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, r, s.log, http.StatusOK, body)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+res.Job.ID.String())
	writeJSON(w, r, s.log, http.StatusAccepted, body)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDPath(r, "id")
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	j, err := s.st.GetJob(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	attempts, err := s.st.ListAttempts(r.Context(), id, recentAttempts)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	if attempts == nil {
		attempts = []domain.Attempt{}
	}
	writeJSON(w, r, s.log, http.StatusOK, toJobResponse(j, attempts))
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r)
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	f := store.ListJobsFilter{Limit: limit}
	q := r.URL.Query()
	if v := q.Get("status"); v != "" {
		st, err := domain.ParseStatus(v)
		if err != nil {
			s.badRequest(w, r, err)
			return
		}
		f.Status = &st
	}
	if v := q.Get("type"); v != "" {
		f.Type = &v
	}
	jobs, err := s.st.ListJobs(r.Context(), f)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	out := make([]jobResponse, len(jobs))
	for i, j := range jobs {
		out[i] = toJobResponse(j, nil)
	}
	writeJSON(w, r, s.log, http.StatusOK, map[string]any{"jobs": out, "count": len(out)})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDPath(r, "id")
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	j, err := s.st.CancelJob(r.Context(), id)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, r, s.log, http.StatusConflict, codeConflict, "job is "+string(j.Status))
		return
	}
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, r, s.log, http.StatusOK, map[string]any{"job_id": j.ID, "status": j.Status})
}

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r)
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	entries, err := s.st.ListDLQ(r.Context(), limit)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	out := make([]dlqEntryResponse, len(entries))
	for i, e := range entries {
		out[i] = toDLQResponse(e)
	}
	writeJSON(w, r, s.log, http.StatusOK, map[string]any{"entries": out, "count": len(out)})
}

func (s *Server) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDPath(r, "id")
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	newID, ok := s.newID(w, r)
	if !ok {
		return
	}
	j, created, err := s.st.ReplayDLQJob(r.Context(), id, newID)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	body := replayResponse{JobID: j.ID, ReplayOf: id, Status: j.Status}
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, r, s.log, http.StatusOK, body)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID.String())
	writeJSON(w, r, s.log, http.StatusAccepted, body)
}

type drainRequest struct {
	Mode string `json:"mode"`
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	var req drainRequest
	if err := decodeJSON(r, &req, false); err != nil {
		s.badRequest(w, r, err)
		return
	}
	if req.Mode != store.DrainPause && req.Mode != store.DrainCancelPending {
		s.badRequest(w, r, invalid("mode must be %q or %q", store.DrainPause, store.DrainCancelPending))
		return
	}
	res, err := s.st.DrainQueue(r.Context(), req.Mode)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	body := map[string]any{"draining": true, "mode": res.Mode, "running": res.Running}
	if res.Mode == store.DrainCancelPending {
		body["cancelled"] = res.Cancelled
	} else {
		body["queued"] = res.Queued
	}
	s.log.Warn("queue draining", "mode", res.Mode, "cancelled", res.Cancelled, "queued", res.Queued, "running", res.Running)
	writeJSON(w, r, s.log, http.StatusOK, body)
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	var empty struct{}
	if err := decodeJSON(r, &empty, true); err != nil {
		s.badRequest(w, r, err)
		return
	}
	if err := s.st.ResumeQueue(r.Context()); err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	s.log.Info("queue resumed")
	writeJSON(w, r, s.log, http.StatusOK, map[string]bool{"draining": false})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.st.GetQueueStats(r.Context(), s.cfg.WorkerStaleAfter, s.cfg.StatsWindow)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, r, s.log, http.StatusOK, toStatsResponse(st))
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := decodeJSON(r, &req, false); err != nil {
		s.badRequest(w, r, err)
		return
	}
	v, err := validateSchedule(req, s.jobTypes)
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	id, ok := s.newID(w, r)
	if !ok {
		return
	}
	sc, err := s.st.CreateSchedule(r.Context(), store.NewSchedule{
		ID: id, Name: v.Name, CronExpr: v.CronExpr, Type: v.Type, Payload: v.Payload, Priority: v.Priority,
		MaxRetries: v.MaxRetries, TimeoutSeconds: v.TimeoutSeconds, Enabled: v.Enabled,
	})
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/schedules/"+sc.ID.String())
	writeJSON(w, r, s.log, http.StatusCreated, toScheduleResponse(sc))
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r)
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	scs, err := s.st.ListSchedules(r.Context(), limit)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	out := make([]scheduleResponse, len(scs))
	for i, sc := range scs {
		out[i] = toScheduleResponse(sc)
	}
	writeJSON(w, r, s.log, http.StatusOK, map[string]any{"schedules": out, "count": len(out)})
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUIDPath(r, "id")
	if err != nil {
		s.badRequest(w, r, err)
		return
	}
	if err := s.st.DeleteSchedule(r.Context(), id); err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
