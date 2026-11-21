package controlplane

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
)

const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), metricsTimeout)
	defer cancel()
	st, err := s.st.GetQueueStats(ctx, s.cfg.WorkerStaleAfter, s.cfg.StatsWindow)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	body := renderMetrics(st, s.reqs.snapshot())
	w.Header().Set("Content-Type", metricsContentType)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		s.log.Warn("write metrics failed", "err", err)
	}
}

type promWriter struct{ buf bytes.Buffer }

func (p *promWriter) header(name, typ, help string) {
	fmt.Fprintf(&p.buf, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (p *promWriter) sample(name string, labels [][2]string, v float64) {
	p.buf.WriteString(name)
	if len(labels) > 0 {
		p.buf.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				p.buf.WriteByte(',')
			}
			fmt.Fprintf(&p.buf, "%s=\"%s\"", l[0], escapeLabel(l[1]))
		}
		p.buf.WriteByte('}')
	}
	p.buf.WriteByte(' ')
	p.buf.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	p.buf.WriteByte('\n')
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeLabel(v string) string { return labelEscaper.Replace(v) }

func gauge(p *promWriter, name, help string, v float64) {
	p.header(name, "gauge", help)
	p.sample(name, nil, v)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// renderMetrics writes the Prometheus text exposition (format 0.0.4) for one stats snapshot.
func renderMetrics(st store.QueueStats, reqs []requestCount) []byte {
	var p promWriter

	p.header("jobq_jobs", "gauge", "Queued and running jobs by status and priority.")
	for _, d := range st.ByPriority {
		prio := strconv.Itoa(d.Priority)
		p.sample("jobq_jobs", [][2]string{{"status", string(domain.StatusQueued)}, {"priority", prio}}, float64(d.Queued))
		p.sample("jobq_jobs", [][2]string{{"status", string(domain.StatusRunning)}, {"priority", prio}}, float64(d.Running))
	}

	p.header("jobq_jobs_by_status", "gauge", "Jobs by lifecycle status.")
	for _, status := range domain.AllStatuses {
		p.sample("jobq_jobs_by_status", [][2]string{{"status", string(status)}}, float64(st.ByStatus[status]))
	}

	gauge(&p, "jobq_queue_ready", "Queued jobs whose run_at has passed.", float64(st.Ready))
	gauge(&p, "jobq_queue_delayed", "Queued jobs scheduled for the future.", float64(st.Delayed))
	gauge(&p, "jobq_active_workers", "Workers with a fresh heartbeat.", float64(st.ActiveWorkers))
	gauge(&p, "jobq_total_concurrency", "Sum of concurrency across active workers.", float64(st.TotalConcurrency))
	gauge(&p, "jobq_active_jobs", "Attempts executing across active workers.", float64(st.ActiveJobs))
	gauge(&p, "jobq_worker_utilization", "active_jobs / total_concurrency.", st.WorkerUtilization)

	p.header("jobq_attempt_latency_ms", "gauge", "Attempt execution latency percentiles over the stats window.")
	if st.P50LatencyMS != nil {
		p.sample("jobq_attempt_latency_ms", [][2]string{{"quantile", "0.5"}}, *st.P50LatencyMS)
	}
	if st.P95LatencyMS != nil {
		p.sample("jobq_attempt_latency_ms", [][2]string{{"quantile", "0.95"}}, *st.P95LatencyMS)
	}

	gauge(&p, "jobq_dead_letter_rate", "Dead-lettered share of jobs finished in the stats window.", st.DeadLetterRate)
	gauge(&p, "jobq_dead_letter_jobs", "Rows in the dead-letter queue.", float64(st.DeadLetterCount))

	p.header("jobq_attempts_window", "gauge", "Attempts finished in the stats window by outcome.")
	for _, a := range domain.AllAttemptStatuses {
		if a == domain.AttemptRunning {
			continue
		}
		p.sample("jobq_attempts_window", [][2]string{{"status", string(a)}}, float64(st.AttemptsInWindow[a]))
	}

	gauge(&p, "jobq_draining", "1 while the queue is draining.", b2f(st.Draining))

	p.header("jobq_http_requests_total", "counter", "HTTP requests served by this API process.")
	for _, rc := range reqs {
		p.sample("jobq_http_requests_total", [][2]string{{"method", rc.method}, {"route", rc.route}, {"code", strconv.Itoa(rc.code)}}, float64(rc.n))
	}
	return p.buf.Bytes()
}
