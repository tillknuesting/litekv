package main

// What to break, and what to call it.
//
// Each entry names a file, says what the break means, and gives the exact text
// to replace and what to replace it with. The text is matched exactly and must
// appear exactly once — see the mutation traps in AGENTS.md for every way that
// goes wrong quietly.
//
// A bare file name is a file under server/. A name with a slash is relative to
// the repository, which is how an engine file is written: "./db.go". See locate
// in main.go for why the two are not interchangeable.

type mutation struct {
	file string
	name string
	old  string
	new  string
}

var mutations = []mutation{
	{
		file: "keys.go",
		name: "HEAD sends the value anyway",
		old:  "\tif r.Method == http.MethodHead {",
		new:  "\tif false {",
	},
	{
		file: "keys.go",
		name: "a declared oversize body is read before it is refused",
		old:  "\tif r.ContentLength > s.opts.MaxValue {",
		new:  "\tif false {",
	},
	{
		file: "keys.go",
		name: "the expiry header is stored as an ordinary write",
		old:  "\tif expires.IsZero() {",
		new:  "\tif true {",
	},
	{
		file: "keys.go",
		name: "an unparseable expiry header is ignored",
		old:  "\t\treturn time.Time{}, badRequest(\"%s must be an RFC 3339 time\", headerExpires)",
		new:  "\t\treturn time.Time{}, nil",
	},
	{
		file: "keys.go",
		name: "no Content-Length on a value",
		old:  "\tw.Header().Set(\"Content-Length\", strconv.Itoa(len(value)))",
		new:  "\t_ = strconv.Itoa",
	},
	{
		file: "keys.go",
		name: "a delete of a missing key is a 404",
		old:  "\tw.WriteHeader(s.wrote(w, r))\n}\n\n// expiryOf",
		new:  "\tw.WriteHeader(http.StatusNotFound)\n}\n\n// expiryOf",
	},
	{
		file: "errors.go",
		name: "a deleted key is not a 404",
		old:  "\t\terrors.Is(err, litekv.ErrorKeyDeleted),\n",
		new:  "",
	},
	{
		file: "errors.go",
		name: "an expired key is not a 404",
		old:  "\t\terrors.Is(err, litekv.ErrorKeyExpired):",
		new:  "\t\tfalse:",
	},
	{
		file: "errors.go",
		name: "a fenced store answers without its term",
		old:  "\tif errors.Is(err, litekv.ErrorFenced) {",
		new:  "\tif false {",
	},
	{
		file: "errors.go",
		name: "a 500 says nothing at all",
		old:  "\t\tmessage = \"internal error\"",
		new:  "\t\t_ = message",
	},
	{
		file: "errors.go",
		name: "a 500 tells the client what went wrong",
		old:  "\t\tmessage = \"internal error\"",
		new:  "\t\tmessage = err.Error()",
	},
	{
		file: "errors.go",
		name: "a body over the limit is a 500",
		old:  "\tif errors.As(err, &tooBig) {",
		new:  "\tif false && errors.As(err, &tooBig) {",
	},
	{
		file: "errors.go",
		name: "a closed store is a 500",
		old:  "\tcase errors.Is(err, litekv.ErrorClosed):",
		new:  "\tcase false:",
	},
	{
		file: "server.go",
		name: "MaxValue keeps whatever it was given",
		old:  "\tif opts.MaxValue <= 0 {",
		new:  "\tif false {",
	},
	{
		file: "server.go",
		name: "the queue is not in the write path",
		old:  "\t\twrites:    writer,",
		new:  "\t\twrites:    db,",
	},
	{
		file: "server.go",
		name: "Close leaves the writer running",
		old:  "\treturn s.writer.Close()",
		new:  "\treturn nil",
	},
	{
		file: "server.go",
		name: "the Queue option is dropped on the floor",
		old:  "litekv.WriterOptions{Queue: opts.Queue}",
		new:  "litekv.WriterOptions{}",
	},
	{
		file: "keys.go",
		name: "a delete goes round the queue",
		old:  "\tif err := s.writes.Delete(",
		new:  "\tif err := s.db.Delete(",
	},
	{
		file: "keys.go",
		name: "a write goes round the queue",
		old:  "\t\terr = s.writes.Write(key, value)",
		new:  "\t\terr = s.db.Write(key, value)",
	},
	// --- replication over the wire -----------------------------------------
	{
		file: "replica.go",
		name: "a leader hangs up on a diverged follower instead of snapshotting",
		old:  "\t\tif !errors.Is(err, litekv.ErrorDiverged) {",
		new:  "\t\tif true {",
	},
	{
		file: "replica.go",
		name: "the snapshot's hold is released before Follow takes its own",
		old:  "\t\t_, err := s.db.Follow(from, holding, send, until, litekv.ReplicaOptions{})",
		new:  "\t\tif holding != nil {\n\t\t\tholding()\n\t\t}\n\t\t_, err := s.db.Follow(from, nil, send, until, litekv.ReplicaOptions{})",
	},
	{
		file: "replica.go",
		name: "a frame's claimed length is not bounded",
		old:  "\tif most < 0 || length > uint64(most) || length > math.MaxInt64 {",
		new:  "\tif length > math.MaxInt64 {",
	},
	{
		file: "replica.go",
		name: "the payload is allocated at the length claimed",
		old:  "\tpayload := make([]byte, 0, min(length, chunk))",
		new:  "\tpayload := make([]byte, 0, length)",
	},
	{
		file: "replica.go",
		name: "a short payload is taken as a whole frame",
		old:  "\t\t\treturn nil, err\n\t\t}\n\t\tpayload = payload[:len(payload)+room]",
		new:  "\t\t\treturn payload, nil\n\t\t}\n\t\tpayload = payload[:len(payload)+room]",
	},
	{
		file: "replica.go",
		name: "the frames are not flushed as they are cut",
		old:  "\tif err := writeFrame(o.w, kind, at, payload); err != nil {\n\t\treturn err\n\t}\n\to.flusher.Flush()",
		new:  "\tif err := writeFrame(o.w, kind, at, payload); err != nil {\n\t\treturn err\n\t}",
	},
	{
		file: "replica.go",
		name: "a snapshot frame goes out as a batch",
		old:  "\t\t\terr := out.frameFrom(frameSnapshot, from, size, snapshot)",
		new:  "\t\t\terr := out.frameFrom(frameBatch, from, size, snapshot)",
	},
	{
		file: "replica.go",
		name: "the position the batch leads to is not sent with it",
		old:  "\t\treturn out.frame(frameBatch, next, batch)",
		new:  "\t\treturn out.frame(frameBatch, litekv.DBPosition{}, batch)",
	},
	{
		file: "replica.go",
		name: "a from that is not a position is believed",
		old:  "\t\treturn litekv.DBPosition{}, fmt.Errorf(\"%w: %s\", errBadPosition, err)\n\t}\n\n\tvar pos litekv.DBPosition",
		new:  "\t\treturn litekv.DBPosition{}, nil\n\t}\n\n\tvar pos litekv.DBPosition",
	},
	{
		file: "replica.go",
		name: "a position is padded base64 rather than raw",
		old:  "\treturn base64.RawURLEncoding.EncodeToString(encoded), nil",
		new:  "\treturn base64.URLEncoding.EncodeToString(encoded), nil",
	},
	{
		file: "replica.go",
		name: "a stream is handed out after the streams are closed",
		old:  "\tcase <-s.streams:\n\t\t// A server on its way down.",
		new:  "\tcase <-make(chan struct{}):\n\t\t// A server on its way down.",
	},
	{
		file: "replica.go",
		name: "the stream ignores the streams being closed",
		old:  "\t\tcase <-s.streams:\n\t\tcase <-broken:",
		new:  "\t\tcase <-broken:",
	},
	{
		file: "replica.go",
		name: "a follower carrying a newer term does not fence the leader",
		old:  "\tif from.Term > s.db.Term() {",
		new:  "\tif false {",
	},
	{
		file: "replica.go",
		name: "the leader hears the newer term but does not write it down",
		old:  "\t\tif _, noted := s.db.Since(from, io.Discard, litekv.ReplicaOptions{}); noted != nil {",
		new:  "\t\tif _, noted := error(nil), error(nil); noted != nil {",
	},
	{
		file: "follower.go",
		name: "a follower asks from nowhere rather than from where it is",
		old:  "\tat, err := positionParam(f.db.Applied())",
		new:  "\tat, err := positionParam(litekv.DBPosition{})",
	},
	{
		file: "follower.go",
		name: "a follower applies a batch from wherever the leader said",
		old:  "\t\t\tfrom := f.db.Applied()",
		new:  "\t\t\tfrom := next",
	},
	{
		file: "follower.go",
		name: "a snapshot is applied as a batch",
		old:  "\t\tcase frameSnapshot:",
		new:  "\t\tcase frameBatch + 1:",
	},
	{
		file: "follower.go",
		name: "a follower gives up when the connection ends",
		old:  "\t\tselect {\n\t\tcase <-ctx.Done():\n\t\t\treturn\n\t\tcase <-time.After(delay):\n\t\t}",
		new:  "\t\t_ = delay\n\t\treturn",
	},
	{
		file: "follower.go",
		name: "a refusal is retried as fast as an ordinary disconnection",
		old:  "\t\tcase hopeless(err):\n\t\t\twait = f.opts.MaxBackoff",
		new:  "\t\tcase hopeless(err):\n\t\t\twait = f.opts.MinBackoff",
	},
	{
		file: "follower.go",
		name: "the backoff never grows",
		old:  "\t\t\tif wait *= 2; wait > f.opts.MaxBackoff {",
		new:  "\t\t\tif wait *= 1; wait > f.opts.MaxBackoff {",
	},
	{
		file: "follower.go",
		name: "the backoff never comes back down after a good connection",
		old:  "\t\tcase time.Since(started) >= f.opts.MaxBackoff:",
		new:  "\t\tcase started.IsZero():",
	},
	{
		file: "follower.go",
		name: "a status other than 200 is read as a stream",
		old:  "\tif resp.StatusCode != http.StatusOK {",
		new:  "\tif false {",
	},
	{
		file: "follower.go",
		name: "Close does not wait for the goroutine",
		old:  "\t\tf.cancel()\n\t\t<-f.done",
		new:  "\t\tf.cancel()",
	},
	{
		file: "follower.go",
		name: "an unfollowable address is accepted",
		old:  "\tif base.Scheme != \"http\" && base.Scheme != \"https\" {",
		new:  "\tif false {",
	},
	{
		file: "server.go",
		name: "closing the server leaves its streams running",
		old:  "\ts.CloseStreams()\n\n\t// Before the writer",
		new:  "\tif false {\n\t}\n\n\t// Before the writer",
	},
	{
		file: "server.go",
		name: "the replication route is not registered",
		old:  "\ts.handle(\"GET \"+replicaPath, s.streamReplica)",
		new:  "\t_ = replicaPath",
	},
	{
		file: "errors.go",
		name: "a position that could not be read is a 500",
		old:  "\tcase errors.Is(err, errBadPosition):",
		new:  "\tcase false:",
	},
	{
		file: "role.go",
		name: "a replica takes writes",
		old:  "\tleader, replica := s.following()\n\tif !replica {\n\t\treturn true\n\t}",
		new:  "\tleader, replica := s.following()\n\tif !replica || replica {\n\t\t_ = leader\n\t\treturn true\n\t}",
	},
	{
		file: "role.go",
		name: "a refusal does not say where the leader is",
		old:  "\tw.Header().Set(headerLeader, leader)",
		new:  "\t_ = leader",
	},
	{
		file: "role.go",
		name: "following is never noticed",
		old:  "\treturn s.role.leader, s.role.leader != \"\"",
		new:  "\treturn s.role.leader, false",
	},
	{
		file: "role.go",
		name: "promote raises the term before stopping the follower",
		old:  "\tif err := s.stopFollowing(); err != nil {\n\t\treturn 0, err\n\t}\n\treturn s.db.Promote()",
		new:  "\tterm, err := s.db.Promote()\n\tif err != nil {\n\t\treturn 0, err\n\t}\n\tif err := s.stopFollowing(); err != nil {\n\t\treturn 0, err\n\t}\n\treturn term, nil",
	},
	{
		file: "role.go",
		name: "promote does not stop the follower at all",
		old:  "\tif err := s.stopFollowing(); err != nil {\n\t\treturn 0, err\n\t}\n\treturn s.db.Promote()",
		new:  "\treturn s.db.Promote()",
	},
	{
		file: "role.go",
		name: "stopFollowing forgets to clear the leader",
		old:  "\ts.role.leader, s.role.stop = \"\", nil",
		new:  "\ts.role.stop = nil",
	},
	{
		file: "role.go",
		name: "a second leader may be followed",
		old:  "\tif s.role.stop != nil {",
		new:  "\tif false {",
	},
	{
		file: "role.go",
		name: "the After header is ignored",
		old:  "\traw := r.Header.Get(headerAfter)\n\tif raw == \"\" {\n\t\treturn true\n\t}",
		new:  "\traw := r.Header.Get(headerAfter)\n\t_ = raw\n\tif true {\n\t\treturn true\n\t}",
	},
	{
		file: "role.go",
		name: "a stale read is answered instead of refused",
		old:  "\t\tif err := s.db.Reached(pos); err != nil {",
		new:  "\t\tif err := s.db.Reached(pos); false {",
	},
	{
		file: "role.go",
		name: "a wait does not wait",
		old:  "\tif err := s.db.Await(pos, until); err != nil {",
		new:  "\tif err := s.db.Reached(pos); err != nil {",
	},
	{
		file: "role.go",
		name: "the wait header is ignored and nothing waits",
		old:  "\tif wait == 0 {",
		new:  "\tif true {",
	},
	{
		file: "role.go",
		name: "a write hands back no position",
		old:  "\t\tw.Header().Set(headerPosition, encoded)\n\t}",
		new:  "\t\t_ = encoded\n\t}",
	},
	{
		file: "role.go",
		name: "status always says leader",
		old:  "\tif replica {\n\t\tbody.Role = \"replica\"\n\t}",
		new:  "\tif replica && !replica {\n\t\tbody.Role = \"replica\"\n\t}",
	},
	{
		file: "role.go",
		name: "a negative wait is accepted",
		old:  "\tif wait < 0 {",
		new:  "\tif false {",
	},
	{
		file: "keys.go",
		name: "a put does not check the role",
		old:  "\tif !s.mayWrite(w, r) {\n\t\treturn\n\t}\n\n\tkey := []byte(r.PathValue(\"key\"))\n\n\texpires",
		new:  "\tkey := []byte(r.PathValue(\"key\"))\n\n\texpires",
	},
	{
		file: "keys.go",
		name: "a delete does not check the role",
		old:  "\tif !s.mayWrite(w, r) {\n\t\treturn\n\t}\n\n\tif err := s.writes.Delete(",
		new:  "\tif err := s.writes.Delete(",
	},
	{
		file: "keys.go",
		name: "a read ignores the After header",
		old:  "\tif !s.notStale(w, r) {\n\t\treturn\n\t}",
		new:  "\tif false {\n\t\treturn\n\t}",
	},
	{
		file: "batch.go",
		name: "a batch does not check the role",
		old:  "\tif !s.mayWrite(w, r) {\n\t\treturn\n\t}\n\n\t// Refused on the declared length",
		new:  "\t// Refused on the declared length",
	},
	{
		file: "scan.go",
		name: "a range ignores the After header",
		old:  "\tif !s.notStale(w, r) {\n\t\treturn\n\t}",
		new:  "\tif false {\n\t\treturn\n\t}",
	},
	{
		file: "server.go",
		name: "closing the server leaves the follower running",
		old:  "\tif err := s.stopFollowing(); err != nil {\n\t\ts.log.Error(\"stopping the follower\", \"err\", err)\n\t}",
		new:  "\t{\n\t}",
	},
	{
		file: "ops.go",
		name: "the token check lets everything through",
		old:  "\tif s.opts.Token == \"\" {\n\t\treturn true\n\t}",
		new:  "\tif s.opts.Token == \"\" || s.opts.Token != \"\" {\n\t\treturn true\n\t}",
	},
	{
		file: "ops.go",
		name: "the token covers the health check too",
		old:  "\tif r.URL.Path == healthPath {\n\t\treturn true\n\t}",
		new:  "\tif false {\n\t\treturn true\n\t}",
	},
	{
		file: "ops.go",
		name: "every route is exempt, not just health",
		old:  "\tif r.URL.Path == healthPath {\n\t\treturn true\n\t}",
		new:  "\tif r.URL.Path != healthPath || true {\n\t\treturn true\n\t}",
	},
	{
		file: "ops.go",
		name: "a wrong token is accepted if it is a prefix",
		old:  "\treturn subtle.ConstantTimeCompare(\n\t\t[]byte(strings.TrimPrefix(given, prefix)), []byte(s.opts.Token)) == 1",
		new:  "\t_ = subtle.ConstantTimeCompare\n\treturn strings.HasPrefix(s.opts.Token, strings.TrimPrefix(given, prefix))",
	},
	{
		file: "ops.go",
		name: "nothing is counted",
		old:  "\t\ts.metrics.observe(route, r.Method, counted.status, took)",
		new:  "\t\t_ = took",
	},
	{
		file: "ops.go",
		name: "the status a handler sent is not remembered",
		old:  "\tif !a.wrote {\n\t\ta.status, a.wrote = status, true\n\t}",
		new:  "\tif !a.wrote {\n\t\ta.wrote = true\n\t}",
	},
	{
		file: "ops.go",
		name: "health does not ask the store anything",
		old:  "\tif err := s.db.Reached(litekv.DBPosition{}); err != nil {",
		new:  "\tif err := s.db.Reached(litekv.DBPosition{}); false {",
	},
	{
		file: "ops.go",
		name: "a bucket counts only what falls exactly in it",
		old:  "\t\tif seconds <= at {",
		new:  "\t\tif seconds == at {",
	},
	{
		file: "server.go",
		name: "the route label is the path rather than the pattern",
		old:  "\troute := pattern\n\tif _, path, found := strings.Cut(pattern, \" \"); found {\n\t\troute = path\n\t}",
		new:  "\troute := pattern\n\tif _, path, found := strings.Cut(pattern, \" \"); found {\n\t\troute = path\n\t}\n\troute = \"/\"",
	},
	{
		file: "server.go",
		name: "the token is not checked at all",
		old:  "\tif !s.allowed(r) {\n\t\tunauthorized(w)\n\t\treturn\n\t}",
		new:  "\tif false {\n\t\tunauthorized(w)\n\t\treturn\n\t}",
	},
	{
		file: "replica.go",
		name: "a stream keeps the server's write deadline",
		old:  "\tif err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {",
		new:  "\tif err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Second)); err != nil {",
	},
	{
		file: "follower.go",
		name: "a follower does not present its token",
		old:  "\tif f.opts.Token != \"\" {\n\t\treq.Header.Set(\"Authorization\", \"Bearer \"+f.opts.Token)\n\t}",
		new:  "\tif false {\n\t\treq.Header.Set(\"Authorization\", \"Bearer \"+f.opts.Token)\n\t}",
	},
	{
		file: "replica.go",
		name: "a quiet leader never says it is there",
		old:  "\tif beat := s.opts.heartbeat(); beat > 0 {",
		new:  "\tif beat := s.opts.heartbeat(); false {",
	},
	{
		file: "replica.go",
		name: "a heartbeat goes out as a batch",
		old:  "\t\t\t\t\tif err := out.frame(frameHeartbeat, s.db.Position(), nil); err != nil {",
		new:  "\t\t\t\t\tif err := out.frame(frameBatch, s.db.Position(), nil); err != nil {",
	},
	{
		file: "replica.go",
		name: "a heartbeat that fails to go out is not noticed",
		old:  "\t\t\t\t\t\tonce.Do(func() { close(broken) })",
		new:  "\t\t\t\t\t\tonce.Do(func() { _ = broken })",
	},
	{
		file: "follower.go",
		name: "a follower waits on a silent leader for ever",
		old:  "\tif f.opts.Idle > 0 {",
		new:  "\tif false {",
	},
	{
		file: "follower.go",
		name: "the idle deadline moves on frames rather than on bytes",
		old:  "\t\tbody = &alive{r: resp.Body, beat: func() { silent.Reset(f.opts.Idle) }}",
		new:  "\t\t_ = &alive{r: resp.Body, beat: func() { silent.Reset(f.opts.Idle) }}",
	},
	{
		file: "follower.go",
		name: "a heartbeat is taken for a frame the follower does not know",
		old:  "\t\tcase frameHeartbeat:",
		new:  "\t\tcase 'Z':",
	},
	{
		file: "replica.go",
		name: "the handler does not wait for its heartbeat goroutine",
		old:  "\tvar beating sync.WaitGroup\n\tdefer beating.Wait()",
		new:  "\tvar beating sync.WaitGroup",
	},
	{
		file: "replica.go",
		name: "an open stream is not counted",
		old:  "\ts.streaming.Add(1)\n\tdefer s.streaming.Add(-1)",
		new:  "\ts.streaming.Add(0)",
	},
	{
		file: "replica.go",
		name: "a stream that ended is still counted",
		old:  "\ts.streaming.Add(1)\n\tdefer s.streaming.Add(-1)",
		new:  "\ts.streaming.Add(1)",
	},
	{
		file: "acks.go",
		name: "a write does not wait for anybody",
		old:  "\t\tgot, changed := f.count(want)\n\t\tif got >= need {",
		new:  "\t\tgot, changed := f.count(want)\n\t\tif true || got >= need {",
	},
	{
		file: "acks.go",
		name: "a follower behind the write counts as having it",
		old:  "\treturn acked.Log.Seq >= want.Log.Seq",
		new:  "\treturn true",
	},
	{
		file: "acks.go",
		name: "the term is not checked, so another leader's follower counts",
		old:  "\tif acked.Term != want.Term {\n\t\treturn acked.Term > want.Term\n\t}",
		new:  "\tif false {\n\t\treturn acked.Term > want.Term\n\t}",
	},
	{
		file: "acks.go",
		name: "an ack from a stranger is taken",
		old:  "\twas, streaming := f.at[id]\n\tif !streaming {\n\t\treturn false\n\t}",
		new:  "\twas := f.at[id]",
	},
	{
		file: "acks.go",
		name: "an ack out of order takes a follower backwards",
		old:  "\tif reaches(was, at) {\n\t\treturn true // an ack that arrived out of order, which a retry can do\n\t}",
		new:  "\t_ = was",
	},
	{
		file: "acks.go",
		name: "a follower that went away keeps counting",
		old:  "\tdelete(f.at, id)\n\tf.wake()",
		new:  "\tf.wake()",
	},
	{
		file: "acks.go",
		name: "nothing waiting is ever woken",
		old:  "\tclose(f.changed)\n\tf.changed = make(chan struct{})",
		new:  "\tif false {\n\t\tclose(f.changed)\n\t}\n\tf.changed = make(chan struct{})",
	},
	{
		file: "role.go",
		name: "a write that was not replicated still answers 204",
		old:  "\treturn http.StatusAccepted\n}",
		new:  "\treturn http.StatusNoContent\n}",
	},
	{
		file: "role.go",
		name: "the number of followers that had it is not reported",
		old:  "\tw.Header().Set(headerReplicated, strconv.Itoa(got))",
		new:  "\t_ = strconv.Itoa(got)",
	},
	{
		file: "role.go",
		name: "WaitFor unset still waits",
		old:  "\tif need <= 0 {\n\t\treturn http.StatusNoContent\n\t}",
		new:  "\tif false {\n\t\treturn http.StatusNoContent\n\t}",
	},
	{
		file: "replica.go",
		name: "a stream does not register its follower",
		old:  "\ts.followers.attach(id, from)\n\tdefer s.followers.detach(id)",
		new:  "\t_ = id",
	},
	{
		file: "follower.go",
		name: "a follower never says how far it has got",
		old:  "\tf.latest.Store(&at)",
		new:  "\t_ = at",
	},
	{
		file: "follower.go",
		name: "a follower does not say who it is",
		old:  "\ttarget.RawQuery = url.Values{\"from\": {at}, \"id\": {f.id}}.Encode()",
		new:  "\ttarget.RawQuery = url.Values{\"from\": {at}}.Encode()",
	},
	{
		file: "role.go",
		name: "a fenced store does not say so in its status",
		old:  "\t\tFenced: s.db.Fenced(), Segments: s.db.Segments(), Keys: s.db.Len()}",
		new:  "\t\tSegments: s.db.Segments(), Keys: s.db.Len()}",
	},
	{
		file: "ops.go",
		name: "the fenced gauge is always zero",
		old:  "\t\t\tboolean(s.db.Fenced())},",
		new:  "\t\t\t0},",
	},
	{
		file: "ops.go",
		name: "the fenced gauge is always one",
		old:  "\tif yes {\n\t\treturn 1\n\t}\n\treturn 0",
		new:  "\t_ = yes\n\treturn 1",
	},
	{
		file: "replica.go",
		name: "the spool file is left on the disk",
		old:  "\tif err := os.Remove(file.Name()); err != nil {\n\t\t_ = file.Close()\n\t\treturn nil, 0, litekv.DBPosition{}, nil, err\n\t}",
		new:  "\tif false {\n\t\t_ = file.Close()\n\t}",
	},
	{
		file: "replica.go",
		name: "the spool file is not rewound before it is sent",
		old:  "\tsize, err := file.Seek(0, io.SeekEnd)\n\tif err == nil {\n\t\t_, err = file.Seek(0, io.SeekStart)\n\t}",
		new:  "\tsize, err := file.Seek(0, io.SeekEnd)",
	},
	{
		file: "replica.go",
		name: "a short snapshot goes out as if it were whole",
		old:  "\tif sent != length {",
		new:  "\tif false {",
	},
	{
		file: "follower.go",
		name: "a snapshot is read into memory rather than streamed",
		old:  "\t\t\tif err := f.take(next, body, length, opts); err != nil {\n\t\t\t\treturn err\n\t\t\t}",
		new:  "\t\t\twhole, err := readPayload(body, length)\n\t\t\tif err != nil {\n\t\t\t\treturn err\n\t\t\t}\n\t\t\tif err := f.db.ApplySnapshot(next, bytes.NewReader(whole), opts); err != nil {\n\t\t\t\treturn err\n\t\t\t}",
	},
	{
		file: "follower.go",
		name: "what a snapshot did not read is left on the connection",
		old:  "\tif _, err := io.Copy(io.Discard, payload); err != nil {\n\t\treturn err\n\t}\n\treturn applied",
		new:  "\treturn applied",
	},
	{
		file: "follower.go",
		name: "a heartbeat may carry a payload",
		old:  "\t\t\tif length != 0 {\n\t\t\t\treturn fmt.Errorf(\"a heartbeat carrying %d bytes\", length)\n\t\t\t}",
		new:  "\t\t\t_ = length",
	},
	// --- one process owns a directory ---------------------------------------
	// Engine files, named with a leading ./ — see locate() in mutate.py. These
	// run the engine's suite, which is fifteen times the server's, so a sweep
	// that includes them is minutes rather than seconds.
	{
		file: "./lock_flock.go",
		name: "the lock is shared rather than exclusive",
		old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		new:  "syscall.LOCK_SH|syscall.LOCK_NB",
	},
	{
		file: "./db.go",
		name: "the lock is taken after the directory has been read",
		old:  "\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
		new:  "\tif _, err := segmentIDs(dir); err != nil {\n\t\treturn nil, err\n\t}\n\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		file: "./db.go",
		name: "an open that fails keeps the lock",
		old:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\tlock.Unlock()\n\t\treturn nil, err\n\t}",
		new:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		file: "./db.go",
		name: "Close lets go of the lock before it closes the logs",
		old:  "\terr := db.closeSegments()\n",
		new:  "\tif uerr := db.lock.Unlock(); uerr != nil {\n\t\treturn uerr\n\t}\n\terr := db.closeSegments()\n",
	},
	{
		file: "./db.go",
		name: "Close never lets go of the lock",
		old:  "\tif uerr := db.lock.Unlock(); err == nil {\n\t\terr = uerr\n\t}",
		new:  "\t_ = db.lock",
	},
	// Both filters, not one. LOCK is turned away twice over — it has no .seg
	// suffix and its name is not a number — and removing either alone changes
	// nothing, which is two checks hiding each other in the way AGENTS.md
	// describes for latestOffsets. The mutation has to take both.
	{
		file: "./db.go",
		name: "the lock file is counted as a log",
		old:  "\t\tif entry.IsDir() || !strings.HasSuffix(name, segmentSuffix) {\n\t\t\tcontinue\n\t\t}\n\t\tid, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)\n\t\tif err != nil {\n\t\t\tcontinue // not ours\n\t\t}",
		new:  "\t\tif entry.IsDir() {\n\t\t\tcontinue\n\t\t}\n\t\tid, _ := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)",
	},
	// The errors.Is call is left standing and only its result widened: taking
	// the call out leaves "errors" imported and not used, which is a mutation
	// that does not build and therefore never ran. See the mutation traps in
	// AGENTS.md.
	{
		file: "./lock_flock.go",
		name: "any failure to lock is reported as somebody else holding it",
		old:  "\treturn errors.Is(err, syscall.EWOULDBLOCK)\n",
		new:  "\treturn errors.Is(err, syscall.EWOULDBLOCK) || true\n",
	},
	{
		file: "./lock_flock.go",
		name: "the lock is taken but waited for rather than refused",
		old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		new:  "syscall.LOCK_EX",
	},
	// There is deliberately no mutation for dropping the explicit LOCK_UN from
	// Unlock. Closing the descriptor releases the lock by itself, so a version
	// without it behaves identically — an equivalent mutant, not a mutation,
	// and one that would sit in the survivor list forever implying a missing
	// test. The explicit call is there for the ordering, which
	// TestTheLockIsReleasedAfterTheLastLogIsClosed checks through the seam.
}
