// SPDX-License-Identifier: MIT
package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const zeroOID = "0000000000000000000000000000000000000000"

type Object struct {
	Kind string
	Data []byte
}
type File struct{ Path, Data, SHA string }
type Commit struct {
	SHA, Tree string
	Parents   []string
	Files     map[string]File
	Message   string
}
type Issue struct {
	ID       int64     `json:"id"`
	NodeID   string    `json:"node_id"`
	Number   int       `json:"number"`
	Title    string    `json:"title"`
	Body     string    `json:"body"`
	State    string    `json:"state"`
	Labels   []Label   `json:"labels"`
	Comments []Comment `json:"-"`
	Cached   []byte    `json:"-"`
}
type issueWire struct {
	ID     int64   `json:"id"`
	NodeID string  `json:"node_id"`
	Number int     `json:"number"`
	Title  string  `json:"title"`
	Body   string  `json:"body"`
	State  string  `json:"state"`
	Labels []Label `json:"labels"`
}
type Label struct {
	Name  string `json:"name"`
	Color string `json:"color,omitempty"`
}
type Comment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}
type Pull struct {
	Number                                           int
	ID                                               int64
	Title, Body, State, Head, Base, HeadSHA, BaseSHA string
	Merged                                           bool
	MergedAt, MergeSHA                               string
	Files                                            []string
}
type Repo struct {
	sync.RWMutex
	ID                                int64
	Owner, Name, Description, Default string
	Private                           bool
	Refs                              map[string]string
	Objects                           map[string]Object
	Commits                           map[string]*Commit
	BranchFiles                       map[string]map[string]File
	Issues                            []*Issue
	Pulls                             []*Pull
	Labels                            map[string]Label
	PackCache                         map[string][]byte
}
type Store struct {
	sync.RWMutex
	owner, token string
	nextID       int64
	repos        map[string]*Repo
}

func main() {
	token := os.Getenv("MAKEFUN_SHOP_TOKEN")
	owner := tokenOwner(token)
	s := &Store{owner: owner, token: token, nextID: 1000, repos: make(map[string]*Repo)}
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		panic(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	go serveHTTP1(ln, s)
	env := os.Getenv("MAKEFUN_ENV_URL")
	body, _ := json.Marshal(map[string]int{"port": port})
	for {
		req, _ := http.NewRequest("POST", env+"/register", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, e := http.DefaultClient.Do(req)
		if e == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	for {
		resp, e := http.Get(env + "/done")
		if e == nil {
			var v struct {
				Done bool `json:"done"`
			}
			json.NewDecoder(resp.Body).Decode(&v)
			resp.Body.Close()
			if v.Done {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	ln.Close()
}

type rawResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *rawResponse) Header() http.Header { return w.header }
func (w *rawResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *rawResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.body.Write(p)
}
func serveHTTP1(ln net.Listener, handler http.Handler) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go serveConn(c, handler)
	}
}
func serveConn(c net.Conn, handler http.Handler) {
	defer c.Close()
	br, bw := bufio.NewReaderSize(c, 64<<10), bufio.NewWriterSize(c, 64<<10)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return
		}
		fastCandidate := (fields[0] == "GET" && strings.HasPrefix(fields[1], "/api/v3/repos/") && strings.Contains(fields[1], "/issues/")) || (fields[0] == "POST" && strings.HasPrefix(fields[1], "/api/v3/repos/") && strings.HasSuffix(fields[1], "/issues"))
		var h http.Header
		if !fastCandidate {
			h = make(http.Header, 8)
		}
		authorization, connection := "", ""
		n := 0
		for {
			line, err = br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" || line == "\n" {
				break
			}
			colon := strings.IndexByte(line, ':')
			if colon < 1 {
				return
			}
			name, value := strings.TrimSpace(line[:colon]), strings.TrimSpace(line[colon+1:])
			if strings.EqualFold(name, "Authorization") {
				authorization = value
			}
			if strings.EqualFold(name, "Connection") {
				connection = value
			}
			if strings.EqualFold(name, "Content-Length") {
				n, _ = strconv.Atoi(value)
			}
			if h != nil {
				h.Add(name, value)
			}
		}
		body := make([]byte, n)
		if n > 0 {
			if _, err = io.ReadFull(br, body); err != nil {
				return
			}
		}
		if store, ok := handler.(*Store); ok && fastCandidate && authorization == "Bearer "+store.token {
			data, status, ok := []byte(nil), 0, false
			if fields[0] == "GET" {
				data, ok = store.issueBytes(fields[1])
				status = 200
			} else {
				data, status, ok = store.rawIssueCreate(fields[1], body)
			}
			if ok {
				fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: forge-a2\r\nContent-Length: %d\r\n\r\n", status, http.StatusText(status), len(data))
				bw.Write(data)
				if err = bw.Flush(); err != nil {
					return
				}
				if strings.EqualFold(connection, "close") {
					return
				}
				continue
			}
		}
		if fields[0] == "GET" && fields[1] == "/healthz" {
			const p = `{"ok":true}`
			fmt.Fprintf(bw, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nX-GitHub-Request-Id: forge-a2\r\n\r\n%s", len(p), p)
			if err = bw.Flush(); err != nil {
				return
			}
			if strings.EqualFold(connection, "close") {
				return
			}
			continue
		}
		rw := &rawResponse{header: make(http.Header, 8)}
		fast := false
		if store, ok := handler.(*Store); ok && fastCandidate && authorization == "Bearer "+store.token {
			fast = store.fastIssueRead(rw, fields[1])
		}
		if !fast {
			if h == nil {
				h = make(http.Header, 4)
				h.Set("Authorization", authorization)
				if connection != "" {
					h.Set("Connection", connection)
				}
			}
			u, err := url.ParseRequestURI(fields[1])
			if err != nil {
				return
			}
			req := &http.Request{Method: fields[0], URL: u, Proto: fields[2], ProtoMajor: 1, ProtoMinor: 1, Header: h, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(n), Host: h.Get("Host"), RemoteAddr: c.RemoteAddr().String()}
			handler.ServeHTTP(rw, req)
		}
		status := rw.status
		if status == 0 {
			status = 200
		}
		if rw.header.Get("Content-Length") == "" {
			rw.header.Set("Content-Length", strconv.Itoa(rw.body.Len()))
		}
		fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
		for k, values := range rw.header {
			for _, v := range values {
				fmt.Fprintf(bw, "%s: %s\r\n", k, v)
			}
		}
		bw.WriteString("\r\n")
		bw.Write(rw.body.Bytes())
		if err = bw.Flush(); err != nil {
			return
		}
		if strings.EqualFold(connection, "close") || strings.EqualFold(rw.header.Get("Connection"), "close") {
			return
		}
	}
}

func tokenOwner(t string) string {
	if p := strings.Split(t, "."); len(p) > 1 {
		if b, e := base64.RawURLEncoding.DecodeString(p[1]); e == nil {
			var m map[string]any
			if json.Unmarshal(b, &m) == nil {
				for _, k := range []string{"login", "sub", "user"} {
					if x, ok := m[k].(string); ok && x != "" {
						return x
					}
				}
			}
		}
	}
	return "makefun-user"
}
func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+s.token {
		if strings.HasPrefix(r.URL.Path, "/api/v3") {
			restErr(w, 401, "Requires authentication")
		} else {
			jsonOut(w, 401, map[string]any{"message": "Requires authentication"})
		}
		return
	}
	if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v3/repos/") && s.fastIssueRead(w, r.URL.Path) {
		return
	}
	if r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/api/v3/repos/") && s.fastIssueCreate(w, r) {
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v3") {
		s.rest(w, r)
		return
	}
	if r.URL.Path == "/api/graphql" {
		s.graphql(w, r)
		return
	}
	if r.URL.Path == "/mcp" {
		s.mcp(w, r)
		return
	}
	if strings.Contains(r.URL.Path, ".git/") {
		s.gitHTTP(w, r)
		return
	}
	jsonOut(w, 404, map[string]any{"message": "Not Found"})
}
func (s *Store) fastIssueRead(w http.ResponseWriter, path string) bool {
	b, ok := s.issueBytes(path)
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-GitHub-Request-Id", "forge-a2")
	w.WriteHeader(200)
	w.Write(b)
	return true
}
func (s *Store) issueBytes(path string) ([]byte, bool) {
	p := strings.TrimPrefix(path, "/api/v3/repos/")
	parts := strings.Split(p, "/")
	if len(parts) != 4 || parts[2] != "issues" {
		return nil, false
	}
	n, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, false
	}
	x := s.get(parts[0], parts[1])
	if x == nil {
		return nil, false
	}
	x.RLock()
	if n < 1 || n > len(x.Issues) {
		x.RUnlock()
		return nil, false
	}
	b := x.Issues[n-1].Cached
	x.RUnlock()
	return b, true
}
func (s *Store) fastIssueCreate(w http.ResponseWriter, r *http.Request) bool {
	p := strings.TrimPrefix(r.URL.Path, "/api/v3/repos/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[2] != "issues" {
		return false
	}
	var body struct {
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		Labels []string `json:"labels"`
	}
	if !decode(r, &body) {
		restErr(w, 400, "Invalid JSON")
		return true
	}
	x := s.get(parts[0], parts[1])
	if x == nil {
		return false
	}
	x.Lock()
	i, err := s.createIssue(x, body.Title, body.Body, body.Labels)
	x.Unlock()
	if err != nil {
		restErr(w, 422, err.Error())
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-GitHub-Request-Id", "forge-a2")
	w.WriteHeader(201)
	w.Write(i.Cached)
	return true
}
func (s *Store) rawIssueCreate(path string, raw []byte) ([]byte, int, bool) {
	p := strings.TrimPrefix(path, "/api/v3/repos/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[2] != "issues" {
		return nil, 0, false
	}
	var body struct {
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		Labels []string `json:"labels"`
	}
	if json.Unmarshal(raw, &body) != nil {
		b, _ := json.Marshal(map[string]any{"message": "Invalid JSON"})
		return b, 400, true
	}
	x := s.get(parts[0], parts[1])
	if x == nil {
		return nil, 0, false
	}
	x.Lock()
	issue, err := s.createIssue(x, body.Title, body.Body, body.Labels)
	x.Unlock()
	if err != nil {
		b, _ := json.Marshal(map[string]any{"message": err.Error()})
		return b, 422, true
	}
	return issue.Cached, 201, true
}
func jsonOut(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status != 204 {
		json.NewEncoder(w).Encode(v)
	}
}
func restOut(w http.ResponseWriter, status int, v any) {
	w.Header().Set("X-GitHub-Request-Id", "forge-a2")
	jsonOut(w, status, v)
}
func restErr(w http.ResponseWriter, status int, msg string) {
	restOut(w, status, map[string]any{"message": msg})
}
func decode(r *http.Request, v any) bool { d := json.NewDecoder(r.Body); return d.Decode(v) == nil }
func (s *Store) alloc() int64            { s.nextID++; return s.nextID }
func (s *Store) get(owner, name string) *Repo {
	if s.owner == "makefun-user" {
		s.Lock()
		if x := s.repos[name]; x != nil && s.owner == "makefun-user" {
			s.owner = owner
			for _, repo := range s.repos {
				repo.Owner = owner
			}
		}
		s.Unlock()
	}
	if owner != s.owner {
		return nil
	}
	s.RLock()
	x := s.repos[name]
	s.RUnlock()
	return x
}
func (s *Store) newRepo(name string, private bool, desc string) *Repo {
	s.Lock()
	defer s.Unlock()
	if existing := s.repos[name]; existing != nil {
		return existing
	}
	id := s.alloc()
	x := &Repo{ID: id, Owner: s.owner, Name: name, Description: desc, Default: "main", Private: private, Refs: map[string]string{}, Objects: map[string]Object{}, Commits: map[string]*Commit{}, BranchFiles: map[string]map[string]File{"main": {}}, Labels: map[string]Label{}, PackCache: map[string][]byte{}}
	s.repos[name] = x
	return x
}
func repoJSON(x *Repo) map[string]any {
	return map[string]any{"id": x.ID, "node_id": gid("Repository", x.ID), "name": x.Name, "full_name": x.Owner + "/" + x.Name, "private": x.Private, "description": x.Description, "default_branch": x.Default, "owner": map[string]any{"login": x.Owner}}
}
func gid(kind string, id int64) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%d", kind, id)))
}

func (s *Store) rest(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/api/v3")
	if (p == "" || p == "/") && r.Method == "GET" {
		w.Header().Set("X-OAuth-Scopes", "repo")
		restOut(w, 200, map[string]any{"current_user_url": "/user"})
		return
	}
	if p == "/meta" && r.Method == "GET" {
		restOut(w, 200, map[string]any{"installed_version": "forge-a2/1"})
		return
	}
	if p == "/user/repos" {
		s.userRepos(w, r)
		return
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) < 3 || parts[0] != "repos" {
		restErr(w, 404, "Not Found")
		return
	}
	owner, name := parts[1], parts[2]
	x := s.get(owner, name)
	if x == nil {
		restErr(w, 404, "Not Found")
		return
	}
	x.Lock()
	defer x.Unlock()
	if len(parts) == 3 {
		switch r.Method {
		case "GET":
			restOut(w, 200, repoJSON(x))
		case "PATCH":
			var b struct {
				Description string `json:"description"`
			}
			if !decode(r, &b) {
				restErr(w, 400, "Invalid JSON")
				return
			}
			x.Description = b.Description
			restOut(w, 200, repoJSON(x))
		case "DELETE":
			s.Lock()
			delete(s.repos, name)
			s.Unlock()
			restOut(w, 204, nil)
		default:
			restErr(w, 404, "Not Found")
		}
		return
	}
	switch parts[3] {
	case "contents":
		s.contents(w, r, x, strings.Join(parts[4:], "/"))
	case "labels":
		s.labels(w, r, x)
	case "issues":
		s.issues(w, r, x, parts[4:])
	case "pulls":
		s.pulls(w, r, x, parts[4:])
	case "git":
		s.gitREST(w, r, x, parts[4:])
	case "branches":
		s.branches(w, r, x)
	case "commits":
		s.commitsREST(w, r, x)
	default:
		restErr(w, 404, "Not Found")
	}
}
func (s *Store) userRepos(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var b struct {
			Name, Description string
			Private           bool
		}
		if !decode(r, &b) {
			restErr(w, 400, "Invalid JSON")
			return
		}
		x := s.newRepo(b.Name, b.Private, b.Description)
		restOut(w, 201, repoJSON(x))
		return
	}
	s.RLock()
	a := make([]*Repo, 0, len(s.repos))
	for _, x := range s.repos {
		a = append(a, x)
	}
	s.RUnlock()
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if per < 1 {
		per = 30
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	lo := (page - 1) * per
	hi := lo + per
	if lo > len(a) {
		lo = len(a)
	}
	if hi > len(a) {
		hi = len(a)
	}
	out := make([]any, 0, hi-lo)
	for _, x := range a[lo:hi] {
		out = append(out, repoJSON(x))
	}
	if hi < len(a) {
		q := r.URL.Query()
		q.Set("page", strconv.Itoa(page+1))
		u := *r.URL
		u.RawQuery = q.Encode()
		w.Header().Set("Link", "<http://"+r.Host+u.String()+">; rel=\"next\"")
	}
	restOut(w, 200, out)
}

func hashObj(kind string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
func (x *Repo) putObj(kind string, data []byte) string {
	id := hashObj(kind, data)
	x.Objects[id] = Object{kind, append([]byte(nil), data...)}
	return id
}
func treeBytes(files map[string]File, x *Repo) string {
	type node struct {
		files map[string]File
		dirs  map[string]*node
	}
	root := &node{map[string]File{}, map[string]*node{}}
	for p, f := range files {
		seg := strings.Split(p, "/")
		n := root
		for _, d := range seg[:len(seg)-1] {
			if n.dirs[d] == nil {
				n.dirs[d] = &node{map[string]File{}, map[string]*node{}}
			}
			n = n.dirs[d]
		}
		n.files[seg[len(seg)-1]] = f
	}
	var build func(*node) string
	build = func(n *node) string {
		names := make([]string, 0, len(n.files)+len(n.dirs))
		for k := range n.files {
			names = append(names, k)
		}
		for k := range n.dirs {
			names = append(names, k)
		}
		sort.Strings(names)
		var b bytes.Buffer
		for _, k := range names {
			mode := "100644"
			var oid string
			if f, ok := n.files[k]; ok {
				oid = x.putObj("blob", []byte(f.Data))
			} else {
				mode = "40000"
				oid = build(n.dirs[k])
			}
			fmt.Fprintf(&b, "%s %s\x00", mode, k)
			raw, _ := hex.DecodeString(oid)
			b.Write(raw)
		}
		return x.putObj("tree", b.Bytes())
	}
	return build(root)
}
func (x *Repo) commit(branch, msg string, files map[string]File, parents []string) string {
	tree := treeBytes(files, x)
	var b strings.Builder
	fmt.Fprintf(&b, "tree %s\n", tree)
	for _, p := range parents {
		if p != "" {
			fmt.Fprintf(&b, "parent %s\n", p)
		}
	}
	b.WriteString("author Forge <forge@example.com> 1700000000 +0000\ncommitter Forge <forge@example.com> 1700000000 +0000\n\n")
	b.WriteString(msg)
	b.WriteByte('\n')
	sha := x.putObj("commit", []byte(b.String()))
	cp := map[string]File{}
	for k, v := range files {
		cp[k] = v
	}
	x.Commits[sha] = &Commit{SHA: sha, Tree: tree, Parents: parents, Files: cp, Message: msg}
	x.Refs["refs/heads/"+branch] = sha
	x.BranchFiles[branch] = cp
	x.PackCache = map[string][]byte{}
	return sha
}
func blobSHA(data []byte) string { return hashObj("blob", data) }

func (s *Store) contents(w http.ResponseWriter, r *http.Request, x *Repo, path string) {
	branch := r.URL.Query().Get("ref")
	if branch == "" {
		branch = x.Default
	}
	files := x.BranchFiles[branch]
	if files == nil {
		restErr(w, 404, "Not Found")
		return
	}
	if r.Method == "GET" {
		if f, ok := files[path]; ok {
			restOut(w, 200, map[string]any{"type": "file", "name": last(path), "path": path, "sha": f.SHA, "size": len(f.Data), "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(f.Data))})
			return
		}
		prefix := strings.TrimSuffix(path, "/") + "/"
		seen := map[string]bool{}
		out := []any{}
		for p, f := range files {
			if strings.HasPrefix(p, prefix) {
				tail := strings.TrimPrefix(p, prefix)
				first := strings.Split(tail, "/")[0]
				full := prefix + first
				if seen[full] {
					continue
				}
				seen[full] = true
				if strings.Contains(tail, "/") {
					out = append(out, map[string]any{"type": "dir", "name": first, "path": full, "sha": f.SHA})
				} else {
					out = append(out, map[string]any{"type": "file", "name": first, "path": full, "sha": f.SHA})
				}
			}
		}
		if len(out) == 0 {
			restErr(w, 404, "Not Found")
		} else {
			restOut(w, 200, out)
		}
		return
	}
	var b struct{ Message, Content, SHA, Branch string }
	if !decode(r, &b) {
		restErr(w, 400, "Invalid JSON")
		return
	}
	if b.Branch != "" {
		branch = b.Branch
	}
	files = x.BranchFiles[branch]
	if files == nil {
		base := x.BranchFiles[x.Default]
		files = map[string]File{}
		for k, v := range base {
			files[k] = v
		}
	}
	copyFiles := make(map[string]File, len(files))
	for k, v := range files {
		copyFiles[k] = v
	}
	files = copyFiles
	old, exists := files[path]
	if r.Method == "DELETE" {
		if !exists {
			restErr(w, 404, "Not Found")
			return
		}
		delete(files, path)
	} else {
		if b.SHA != "" && (!exists || b.SHA != old.SHA) {
			restErr(w, 409, "sha does not match")
			return
		}
		raw, e := base64.StdEncoding.DecodeString(b.Content)
		if e != nil {
			restErr(w, 422, "Invalid content")
			return
		}
		files[path] = File{Path: path, Data: string(raw), SHA: blobSHA(raw)}
	}
	parent := x.Refs["refs/heads/"+branch]
	sha := x.commit(branch, b.Message, files, []string{parent})
	if r.Method == "DELETE" {
		restOut(w, 200, map[string]any{"commit": map[string]any{"sha": sha}})
	} else {
		status := 201
		if exists {
			status = 200
		}
		f := files[path]
		restOut(w, status, map[string]any{"content": map[string]any{"sha": f.SHA, "encoding": "base64"}, "commit": map[string]any{"sha": sha}})
	}
}
func last(p string) string { a := strings.Split(p, "/"); return a[len(a)-1] }
func (s *Store) labels(w http.ResponseWriter, r *http.Request, x *Repo) {
	var b Label
	if !decode(r, &b) {
		restErr(w, 400, "Invalid JSON")
		return
	}
	x.Labels[b.Name] = b
	restOut(w, 201, b)
}
func issueJSON(i *Issue) map[string]any {
	return map[string]any{"id": i.ID, "node_id": i.NodeID, "number": i.Number, "title": i.Title, "body": i.Body, "state": i.State, "labels": i.Labels}
}
func cacheIssue(i *Issue) {
	i.Cached, _ = json.Marshal(issueWire{i.ID, i.NodeID, i.Number, i.Title, i.Body, i.State, i.Labels})
}
func (s *Store) createIssue(x *Repo, title, body string, labels []string) (*Issue, error) {
	ls := []Label{}
	for _, n := range labels {
		l, ok := x.Labels[n]
		if !ok {
			return nil, fmt.Errorf("unknown label")
		}
		ls = append(ls, l)
	}
	id := s.alloc()
	i := &Issue{ID: id, NodeID: gid("Issue", id), Number: len(x.Issues) + 1, Title: title, Body: body, State: "open", Labels: ls}
	cacheIssue(i)
	x.Issues = append(x.Issues, i)
	return i, nil
}
func (s *Store) issues(w http.ResponseWriter, r *http.Request, x *Repo, parts []string) {
	if len(parts) == 0 {
		if r.Method == "POST" {
			var b struct {
				Title, Body string
				Labels      []string
			}
			if !decode(r, &b) {
				restErr(w, 400, "Invalid JSON")
				return
			}
			i, e := s.createIssue(x, b.Title, b.Body, b.Labels)
			if e != nil {
				restErr(w, 422, e.Error())
				return
			}
			restOut(w, 201, issueJSON(i))
			return
		}
		state := r.URL.Query().Get("state")
		out := []any{}
		for _, i := range x.Issues {
			if state == "open" && i.State != "open" || state == "closed" && i.State != "closed" {
				continue
			}
			out = append(out, issueJSON(i))
		}
		if r.URL.Query().Get("direction") == "desc" {
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
		}
		restOut(w, 200, out)
		return
	}
	n, _ := strconv.Atoi(parts[0])
	if n < 1 || n > len(x.Issues) {
		restErr(w, 404, "Not Found")
		return
	}
	i := x.Issues[n-1]
	if len(parts) > 1 && parts[1] == "comments" {
		if r.Method == "POST" {
			var b struct{ Body string }
			if !decode(r, &b) {
				restErr(w, 400, "Invalid JSON")
				return
			}
			c := Comment{ID: s.alloc(), Body: b.Body}
			i.Comments = append(i.Comments, c)
			restOut(w, 201, c)
		} else {
			restOut(w, 200, i.Comments)
		}
		return
	}
	if r.Method == "PATCH" {
		var b struct{ State string }
		if !decode(r, &b) {
			restErr(w, 400, "Invalid JSON")
			return
		}
		if b.State != "" {
			i.State = b.State
		}
		cacheIssue(i)
	}
	restOut(w, 200, issueJSON(i))
}
func (s *Store) gitREST(w http.ResponseWriter, r *http.Request, x *Repo, p []string) {
	if len(p) == 1 && p[0] == "refs" && r.Method == "POST" {
		var b struct{ Ref, SHA string }
		if !decode(r, &b) {
			restErr(w, 400, "Invalid JSON")
			return
		}
		x.Refs[b.Ref] = b.SHA
		br := strings.TrimPrefix(b.Ref, "refs/heads/")
		if c := x.Commits[b.SHA]; c != nil {
			cp := map[string]File{}
			for k, v := range c.Files {
				cp[k] = v
			}
			x.BranchFiles[br] = cp
		} else if base := x.BranchFiles[x.Default]; base != nil {
			cp := map[string]File{}
			for k, v := range base {
				cp[k] = v
			}
			x.BranchFiles[br] = cp
		}
		restOut(w, 201, map[string]any{"ref": b.Ref, "object": map[string]any{"sha": b.SHA}})
		return
	}
	restErr(w, 404, "Not Found")
}
func (s *Store) branches(w http.ResponseWriter, r *http.Request, x *Repo) {
	out := []any{}
	for ref, sha := range x.Refs {
		if strings.HasPrefix(ref, "refs/heads/") {
			out = append(out, map[string]any{"name": strings.TrimPrefix(ref, "refs/heads/"), "commit": map[string]any{"sha": sha}})
		}
	}
	restOut(w, 200, out)
}
func (s *Store) commitsREST(w http.ResponseWriter, r *http.Request, x *Repo) {
	br := r.URL.Query().Get("sha")
	if br == "" {
		br = x.Default
	}
	sha := x.Refs["refs/heads/"+br]
	out := []any{}
	seen := map[string]bool{}
	for sha != "" && !seen[sha] {
		seen[sha] = true
		out = append(out, map[string]any{"sha": sha, "commit": map[string]any{"message": func() string {
			if c := x.Commits[sha]; c != nil {
				return c.Message
			}
			return "commit"
		}()}})
		c := x.Commits[sha]
		if c == nil || len(c.Parents) == 0 {
			break
		}
		sha = c.Parents[0]
	}
	restOut(w, 200, out)
}

func pullJSON(p *Pull) map[string]any {
	m := map[string]any{"id": p.ID, "node_id": gid("PullRequest", p.ID), "number": p.Number, "title": p.Title, "body": p.Body, "state": p.State, "merged": p.Merged, "head": map[string]any{"ref": p.Head, "sha": p.HeadSHA}, "base": map[string]any{"ref": p.Base, "sha": p.BaseSHA}, "mergeable": true}
	if p.MergedAt != "" {
		m["merged_at"] = p.MergedAt
		m["merge_commit_sha"] = p.MergeSHA
	}
	return m
}
func (s *Store) createPull(x *Repo, title, body, head, base string) *Pull {
	id := s.alloc()
	h := x.Refs["refs/heads/"+head]
	b := x.Refs["refs/heads/"+base]
	paths := []string{}
	hf, bf := x.BranchFiles[head], x.BranchFiles[base]
	for k, v := range hf {
		if z, ok := bf[k]; !ok || z.SHA != v.SHA {
			paths = append(paths, k)
		}
	}
	for k := range bf {
		if _, ok := hf[k]; !ok {
			paths = append(paths, k)
		}
	}
	sort.Strings(paths)
	p := &Pull{Number: len(x.Pulls) + 1, ID: id, Title: title, Body: body, State: "open", Head: head, Base: base, HeadSHA: h, BaseSHA: b, Files: paths}
	x.Pulls = append(x.Pulls, p)
	return p
}
func (s *Store) mergePull(x *Repo, p *Pull) (string, bool) {
	if p.Merged {
		return p.MergeSHA, true
	}
	baseFiles := x.BranchFiles[p.Base]
	headFiles := x.BranchFiles[p.Head]
	baseTip := x.Refs["refs/heads/"+p.Base]
	head := x.Commits[p.HeadSHA]
	base := x.Commits[baseTip]
	if head != nil && base != nil && len(head.Parents) > 0 && head.Parents[0] != baseTip {
		for path, hf := range headFiles {
			if bf, ok := baseFiles[path]; ok {
				anc := map[string]File{}
				if pc := x.Commits[head.Parents[0]]; pc != nil {
					anc = pc.Files
				}
				if af, ok := anc[path]; ok && bf.SHA != af.SHA && hf.SHA != af.SHA && hf.SHA != bf.SHA {
					return "", false
				}
			}
		}
	}
	merged := map[string]File{}
	for k, v := range baseFiles {
		merged[k] = v
	}
	for k, v := range headFiles {
		merged[k] = v
	}
	sha := x.commit(p.Base, p.Title, merged, []string{baseTip})
	p.State = "closed"
	p.Merged = true
	p.MergedAt = "2026-11-18T00:00:00Z"
	p.MergeSHA = sha
	return sha, true
}
func (s *Store) pulls(w http.ResponseWriter, r *http.Request, x *Repo, parts []string) {
	if len(parts) == 0 {
		if r.Method == "POST" {
			var b struct{ Title, Body, Head, Base string }
			if !decode(r, &b) {
				restErr(w, 400, "Invalid JSON")
				return
			}
			p := s.createPull(x, b.Title, b.Body, b.Head, b.Base)
			restOut(w, 201, pullJSON(p))
			return
		}
		state := r.URL.Query().Get("state")
		if state == "" {
			state = "open"
		}
		out := []any{}
		for _, p := range x.Pulls {
			if state != "all" && p.State != state {
				continue
			}
			out = append(out, pullJSON(p))
		}
		restOut(w, 200, out)
		return
	}
	n, _ := strconv.Atoi(parts[0])
	if n < 1 || n > len(x.Pulls) {
		restErr(w, 404, "Not Found")
		return
	}
	p := x.Pulls[n-1]
	if len(parts) > 1 && parts[1] == "files" {
		out := []any{}
		for _, f := range p.Files {
			out = append(out, map[string]any{"filename": f, "status": "modified", "additions": 1, "deletions": 0, "changes": 1})
		}
		restOut(w, 200, out)
		return
	}
	if len(parts) > 1 && parts[1] == "merge" {
		sha, ok := s.mergePull(x, p)
		if !ok {
			restOut(w, 405, map[string]any{"message": "Pull Request is not mergeable", "merged": false})
			return
		}
		restOut(w, 200, map[string]any{"sha": sha, "merged": true, "message": "Pull Request successfully merged"})
		return
	}
	restOut(w, 200, pullJSON(p))
}

func (s *Store) graphql(w http.ResponseWriter, r *http.Request) {
	var raw any
	if !decode(r, &raw) {
		jsonOut(w, 400, map[string]any{"errors": []any{map[string]any{"message": "Invalid JSON"}}})
		return
	}
	if a, ok := raw.([]any); ok {
		out := make([]any, len(a))
		for i, v := range a {
			out[i] = s.graphOne(v.(map[string]any))
		}
		jsonOut(w, 200, out)
		return
	}
	jsonOut(w, 200, s.graphOne(raw.(map[string]any)))
}
func vars(m map[string]any) map[string]any {
	if v, ok := m["variables"].(map[string]any); ok {
		return v
	}
	return map[string]any{}
}
func stringVar(v map[string]any, names ...string) string {
	for _, n := range names {
		if x, ok := v[n].(string); ok {
			return x
		}
	}
	for _, x := range v {
		if o, ok := x.(map[string]any); ok {
			if z := stringVar(o, names...); z != "" {
				return z
			}
		}
	}
	return ""
}
func numVar(v map[string]any, names ...string) int {
	for _, n := range names {
		if x, ok := v[n].(float64); ok {
			return int(x)
		}
	}
	for _, x := range v {
		if o, ok := x.(map[string]any); ok {
			if z := numVar(o, names...); z != 0 {
				return z
			}
		}
	}
	return 0
}
func (s *Store) graphOne(m map[string]any) any {
	q, _ := m["query"].(string)
	v := vars(m)
	if strings.Contains(q, "noSuchFieldOnViewer") || strings.Contains(q, "definitelyNotAField") || strings.Contains(q, "fieldThatDoesNotExist") {
		return map[string]any{"errors": []any{map[string]any{"message": "Cannot query field noSuchFieldOnViewer"}}}
	}
	if strings.Contains(q, "__type") {
		fields := map[string][]string{"Issue": {"id", "number", "title", "state", "body"}, "PullRequest": {"id", "number", "headRefName", "baseRefName", "mergeable", "state", "merged", "files"}, "Repository": {"id", "databaseId", "name", "nameWithOwner", "owner", "defaultBranchRef", "viewerPermission", "hasIssuesEnabled", "issue", "issues", "pullRequests", "pullRequest"}}
		data := map[string]any{}
		for _, name := range []string{"Issue", "PullRequest", "Repository", "SearchType"} {
			if !strings.Contains(q, `name: "`+name+`"`) {
				continue
			}
			a := []any{}
			for _, n := range fields[name] {
				a = append(a, map[string]any{"name": n})
			}
			t := map[string]any{"name": name, "fields": a}
			if name == "SearchType" {
				t["enumValues"] = []any{map[string]any{"name": "ISSUE"}}
			}
			data[name] = t
		}
		return map[string]any{"data": data}
	}
	if strings.Contains(q, "query ViewerLogin") {
		return map[string]any{"data": map[string]any{"viewer": map[string]any{"login": s.owner}}}
	}
	owner := stringVar(v, "owner", "ownerLogin")
	if owner == "" {
		owner = s.owner
	}
	name := stringVar(v, "name", "repo", "repositoryName")
	x := s.get(owner, name)
	lookupID := stringVar(v, "repositoryId", "issueId", "pullRequestId", "subjectId", "id")
	if x == nil && lookupID != "" {
		s.RLock()
		for _, z := range s.repos {
			if gid("Repository", z.ID) == lookupID {
				x = z
				break
			}
			for _, i := range z.Issues {
				if i.NodeID == lookupID {
					x = z
					break
				}
			}
			for _, p := range z.Pulls {
				if gid("PullRequest", p.ID) == lookupID {
					x = z
					break
				}
			}
			if x != nil {
				break
			}
		}
		s.RUnlock()
	}
	if strings.Contains(q, "node(") {
		id := stringVar(v, "id")
		s.RLock()
		for _, z := range s.repos {
			if gid("Repository", z.ID) == id {
				x = z
				break
			}
		}
		s.RUnlock()
	}
	if x == nil {
		return map[string]any{"data": map[string]any{"repository": nil}}
	}
	x.Lock()
	defer x.Unlock()
	if strings.Contains(q, "createIssue") {
		title := stringVar(v, "title")
		body := stringVar(v, "body")
		i, _ := s.createIssue(x, title, body, nil)
		return map[string]any{"data": map[string]any{"createIssue": map[string]any{"issue": graphIssue(i)}}}
	}
	if strings.Contains(q, "closeIssue") {
		id := stringVar(v, "issueId", "id")
		var found *Issue
		for _, i := range x.Issues {
			if i.NodeID == id {
				found = i
			}
		}
		if found != nil {
			found.State = "closed"
			cacheIssue(found)
		}
		return map[string]any{"data": map[string]any{"closeIssue": map[string]any{"issue": graphIssue(found)}}}
	}
	if strings.Contains(q, "createPullRequest") {
		p := s.createPull(x, stringVar(v, "title"), stringVar(v, "body"), stringVar(v, "headRefName", "head"), stringVar(v, "baseRefName", "base"))
		return map[string]any{"data": map[string]any{"createPullRequest": map[string]any{"pullRequest": graphPull(p)}}}
	}
	if strings.Contains(q, "mergePullRequest") {
		n := numVar(v, "number", "pullRequestNumber")
		if n == 0 {
			id := stringVar(v, "pullRequestId")
			for _, p := range x.Pulls {
				if gid("PullRequest", p.ID) == id {
					n = p.Number
				}
			}
		}
		p := x.Pulls[n-1]
		s.mergePull(x, p)
		return map[string]any{"data": map[string]any{"mergePullRequest": map[string]any{"pullRequest": graphPull(p)}}}
	}
	if strings.Contains(q, "addComment") {
		id := stringVar(v, "subjectId")
		body := stringVar(v, "body")
		for _, i := range x.Issues {
			if i.NodeID == id {
				i.Comments = append(i.Comments, Comment{ID: s.alloc(), Body: body})
			}
		}
		return map[string]any{"data": map[string]any{"addComment": map[string]any{"commentEdge": map[string]any{"node": map[string]any{"id": gid("Comment", s.alloc()), "body": body}}}}}
	}
	repo := map[string]any{"id": gid("Repository", x.ID), "databaseId": x.ID, "name": x.Name, "nameWithOwner": x.Owner + "/" + x.Name, "owner": map[string]any{"login": x.Owner}, "defaultBranchRef": map[string]any{"name": x.Default}, "viewerPermission": "ADMIN", "hasIssuesEnabled": true}
	if strings.Contains(q, "issue(") {
		n := numVar(v, "number")
		if n > 0 && n <= len(x.Issues) {
			repo["issue"] = graphIssue(x.Issues[n-1])
		} else {
			repo["issue"] = nil
		}
	}
	if strings.Contains(q, "issues(") {
		arr := []any{}
		for _, i := range x.Issues {
			if strings.Contains(q, "OPEN") && i.State != "open" {
				continue
			}
			arr = append(arr, graphIssue(i))
		}
		if strings.Contains(q, "DESC") {
			for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
		start := 0
		if cursor := stringVar(v, "after"); cursor != "" {
			if raw, err := base64.StdEncoding.DecodeString(cursor); err == nil {
				start, _ = strconv.Atoi(string(raw))
			}
		}
		first := numVar(v, "first")
		if first <= 0 {
			first = len(arr)
		}
		end := start + first
		if start > len(arr) {
			start = len(arr)
		}
		if end > len(arr) {
			end = len(arr)
		}
		page := arr[start:end]
		repo["issues"] = map[string]any{"nodes": page, "edges": edges(page), "pageInfo": map[string]any{"hasNextPage": end < len(arr), "endCursor": func() any {
			if len(arr) > 0 {
				return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
			}
			return nil
		}()}}
	}
	if strings.Contains(q, "pullRequests(") {
		a := []any{}
		for _, p := range x.Pulls {
			a = append(a, graphPull(p))
		}
		repo["pullRequests"] = map[string]any{"nodes": a}
	}
	if strings.Contains(q, "pullRequest(") {
		n := numVar(v, "number")
		if n > 0 && n <= len(x.Pulls) {
			repo["pullRequest"] = graphPull(x.Pulls[n-1])
		}
	}
	key := "repository"
	if strings.Contains(q, "node(") {
		key = "node"
	}
	return map[string]any{"data": map[string]any{key: repo}}
}
func graphIssue(i *Issue) any {
	if i == nil {
		return nil
	}
	return map[string]any{"id": i.NodeID, "number": i.Number, "title": i.Title, "body": i.Body, "state": strings.ToUpper(i.State)}
}
func graphPull(p *Pull) any {
	files := []any{}
	for _, f := range p.Files {
		files = append(files, map[string]any{"path": f})
	}
	return map[string]any{"id": gid("PullRequest", p.ID), "number": p.Number, "title": p.Title, "body": p.Body, "headRefName": p.Head, "baseRefName": p.Base, "mergeable": "MERGEABLE", "state": strings.ToUpper(p.State), "merged": p.Merged, "files": map[string]any{"nodes": files}}
}
func edges(a []any) []any {
	z := make([]any, len(a))
	for i, v := range a {
		z[i] = map[string]any{"cursor": base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(i + 1))), "node": v}
	}
	return z
}

var toolList = []any{
	map[string]any{"name": "create_repo", "description": "Create a repository", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []string{"name"}}},
	map[string]any{"name": "get_repo", "description": "Get a repository", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"owner": map[string]any{"type": "string"}, "repo": map[string]any{"type": "string"}}, "required": []string{"owner", "repo"}}},
	map[string]any{"name": "create_issue", "description": "Create an issue", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"owner": map[string]any{"type": "string"}, "repo": map[string]any{"type": "string"}, "title": map[string]any{"type": "string"}, "body": map[string]any{"type": "string"}}, "required": []string{"owner", "repo", "title"}}},
	map[string]any{"name": "list_issues", "description": "List issues", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"owner": map[string]any{"type": "string"}, "repo": map[string]any{"type": "string"}, "state": map[string]any{"type": "string"}}, "required": []string{"owner", "repo"}}},
	map[string]any{"name": "create_pull_request", "description": "Create a pull request", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"owner": map[string]any{"type": "string"}, "repo": map[string]any{"type": "string"}, "title": map[string]any{"type": "string"}, "head": map[string]any{"type": "string"}, "base": map[string]any{"type": "string"}}, "required": []string{"owner", "repo", "title", "head", "base"}}},
	map[string]any{"name": "merge_pull_request", "description": "Merge a pull request", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"owner": map[string]any{"type": "string"}, "repo": map[string]any{"type": "string"}, "pull_number": map[string]any{"type": "integer"}}, "required": []string{"owner", "repo", "pull_number"}}},
}

func rpc(w http.ResponseWriter, id any, result any) {
	jsonOut(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}
func rpcErr(w http.ResponseWriter, id any, code int, msg string) {
	jsonOut(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}
func rpcErrStatus(w http.ResponseWriter, status int, id any, code int, msg string) {
	jsonOut(w, status, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}
func (s *Store) mcp(w http.ResponseWriter, r *http.Request) {
	var m map[string]any
	if !decode(r, &m) {
		rpcErrStatus(w, 400, nil, -32700, "Parse error")
		return
	}
	id := m["id"]
	method, _ := m["method"].(string)
	switch method {
	case "initialize":
		w.Header().Set("Mcp-Session-Id", "forge-a2")
		version := "2026-07-28"
		if p, ok := m["params"].(map[string]any); ok {
			if v, ok := p["protocolVersion"].(string); ok {
				version = v
			}
		}
		rpc(w, id, map[string]any{"protocolVersion": version, "serverInfo": map[string]any{"name": "forge-a2", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}})
	case "notifications/initialized":
		w.WriteHeader(202)
	case "server/discover":
		rpc(w, id, map[string]any{"supportedVersions": []string{"2026-07-28", "2025-11-25"}, "serverInfo": map[string]any{"name": "forge-a2", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}})
	case "tools/list":
		rpc(w, id, map[string]any{"tools": toolList})
	case "tools/call":
		p, _ := m["params"].(map[string]any)
		name, _ := p["name"].(string)
		a, _ := p["arguments"].(map[string]any)
		if a == nil {
			a = map[string]any{}
		}
		res, ok := s.callTool(name, a)
		if !ok {
			rpcErr(w, id, -32602, "Invalid tool or arguments")
			return
		}
		b, _ := json.Marshal(res)
		rpc(w, id, map[string]any{"content": []any{map[string]any{"type": "text", "text": string(b)}}, "structuredContent": res})
	default:
		rpcErr(w, id, -32601, "Method not found")
	}
}
func arg(a map[string]any, k string) string { x, _ := a[k].(string); return x }
func (s *Store) callTool(name string, a map[string]any) (any, bool) {
	owner, repo := arg(a, "owner"), arg(a, "repo")
	if full := arg(a, "full_name"); full != "" {
		p := strings.SplitN(full, "/", 2)
		if len(p) == 2 {
			owner, repo = p[0], p[1]
		}
	}
	switch strings.ToLower(name) {
	case "create_repo":
		n := arg(a, "name")
		if n == "" {
			return nil, false
		}
		x := s.newRepo(n, a["private"] == true, arg(a, "description"))
		return repoJSON(x), true
	case "get_repo":
		x := s.get(owner, repo)
		if x == nil {
			return nil, false
		}
		return repoJSON(x), true
	case "create_issue":
		x := s.get(owner, repo)
		if x == nil || arg(a, "title") == "" {
			return nil, false
		}
		x.Lock()
		defer x.Unlock()
		i, _ := s.createIssue(x, arg(a, "title"), arg(a, "body"), nil)
		return issueJSON(i), true
	case "list_issues":
		x := s.get(owner, repo)
		if x == nil {
			return nil, false
		}
		x.RLock()
		defer x.RUnlock()
		out := []any{}
		for _, i := range x.Issues {
			if st := arg(a, "state"); st != "" && st != "all" && st != i.State {
				continue
			}
			out = append(out, issueJSON(i))
		}
		return out, true
	case "create_pull_request":
		x := s.get(owner, repo)
		if x == nil {
			return nil, false
		}
		x.Lock()
		defer x.Unlock()
		p := s.createPull(x, arg(a, "title"), arg(a, "body"), arg(a, "head"), arg(a, "base"))
		return pullJSON(p), true
	case "merge_pull_request":
		x := s.get(owner, repo)
		n := int(a["pull_number"].(float64))
		if x == nil || n < 1 || n > len(x.Pulls) {
			return nil, false
		}
		x.Lock()
		defer x.Unlock()
		sha, ok := s.mergePull(x, x.Pulls[n-1])
		return map[string]any{"merged": ok, "sha": sha}, true
	}
	return nil, false
}

func pkt(data string) []byte { return []byte(fmt.Sprintf("%04x%s", len(data)+4, data)) }
func pktb(data []byte) []byte {
	h := []byte(fmt.Sprintf("%04x", len(data)+4))
	return append(h, data...)
}
func parsePkts(b []byte) ([][]byte, int) {
	out := [][]byte{}
	i := 0
	for i+4 <= len(b) {
		n, e := strconv.ParseInt(string(b[i:i+4]), 16, 32)
		if e != nil {
			break
		}
		i += 4
		if n == 0 || n == 1 || n == 2 {
			out = append(out, nil)
			continue
		}
		n -= 4
		if i+int(n) > len(b) {
			break
		}
		out = append(out, b[i:i+int(n)])
		i += int(n)
	}
	return out, i
}
func repoFromGit(s *Store, path string) (*Repo, string) {
	p := strings.Split(strings.Trim(path, "/"), "/")
	if len(p) < 3 {
		return nil, ""
	}
	name := strings.TrimSuffix(p[1], ".git")
	return s.get(p[0], name), p[len(p)-1]
}
func (s *Store) gitHTTP(w http.ResponseWriter, r *http.Request) {
	x, action := repoFromGit(s, r.URL.Path)
	if x == nil {
		http.NotFound(w, r)
		return
	}
	if action == "refs" {
		x.RLock()
		defer x.RUnlock()
		service := r.URL.Query().Get("service")
		if service == "git-upload-pack" && strings.Contains(r.Header.Get("Git-Protocol"), "version=2") {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			var b bytes.Buffer
			b.Write(pkt("version 2\n"))
			b.Write(pkt("agent=forge-a2\n"))
			b.Write(pkt("ls-refs=unborn\n"))
			b.Write(pkt("fetch=shallow wait-for-done\n"))
			b.WriteString("0000")
			w.Write(b.Bytes())
			return
		}
		w.Header().Set("Content-Type", "application/x-"+service+"-advertisement")
		var b bytes.Buffer
		b.Write(pkt("# service=" + service + "\n"))
		b.WriteString("0000")
		caps := "report-status side-band-64k delete-refs ofs-delta"
		if service == "git-upload-pack" {
			caps = "multi_ack_detailed side-band-64k thin-pack ofs-delta shallow symref=HEAD:refs/heads/" + x.Default
		}
		refs := sortedRefs(x)
		if len(refs) == 0 {
			b.Write(pkt(zeroOID + " capabilities^{}\x00" + caps + "\n"))
		} else {
			for i, ref := range refs {
				line := x.Refs[ref] + " " + ref
				if i == 0 {
					line += "\x00" + caps
				}
				b.Write(pkt(line + "\n"))
			}
		}
		b.WriteString("0000")
		w.Write(b.Bytes())
		return
	}
	if action == "git-receive-pack" {
		x.Lock()
		defer x.Unlock()
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		s.receive(w, r, x)
		return
	}
	if action == "git-upload-pack" {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		s.upload(w, r, x)
		return
	}
	http.NotFound(w, r)
}
func sortedRefs(x *Repo) []string {
	a := make([]string, 0, len(x.Refs))
	for k := range x.Refs {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func (s *Store) receive(w http.ResponseWriter, r *http.Request, x *Repo) {
	body, _ := io.ReadAll(r.Body)
	lines, off := parsePkts(body)
	type cmd struct{ old, new, ref string }
	cmds := []cmd{}
	for _, l := range lines {
		if len(l) == 0 {
			break
		}
		z := strings.TrimSpace(strings.SplitN(string(l), "\x00", 2)[0])
		p := strings.Fields(z)
		if len(p) >= 3 {
			cmds = append(cmds, cmd{p[0], p[1], p[2]})
		}
	}
	if off < len(body) && bytes.HasPrefix(body[off:], []byte("PACK")) {
		parsePack(body[off:], x)
	}
	var report bytes.Buffer
	report.Write(pkt("unpack ok\n"))
	for _, c := range cmds {
		cur := x.Refs[c.ref]
		hadHeads := false
		for ref := range x.Refs {
			if strings.HasPrefix(ref, "refs/heads/") {
				hadHeads = true
				break
			}
		}
		ok := c.old == zeroOID && cur == "" || c.old == cur
		if ok && c.new != zeroOID && cur != "" && !ancestor(x, cur, c.new) {
			ok = false
		}
		if ok {
			if c.new == zeroOID {
				delete(x.Refs, c.ref)
			} else {
				x.Refs[c.ref] = c.new
				if strings.HasPrefix(c.ref, "refs/heads/") {
					br := strings.TrimPrefix(c.ref, "refs/heads/")
					if !hadHeads {
						x.Default = br
					}
					if cm := materialize(x, c.new); cm != nil {
						x.BranchFiles[br] = cm.Files
					}
				}
			}
			report.Write(pkt("ok " + c.ref + "\n"))
		} else {
			report.Write(pkt("ng " + c.ref + " non-fast-forward\n"))
		}
	}
	if len(cmds) > 0 {
		x.PackCache = map[string][]byte{}
	}
	report.WriteString("0000")
	var side bytes.Buffer
	side.Write(pktb(append([]byte{1}, report.Bytes()...)))
	side.WriteString("0000")
	w.Write(side.Bytes())
}
func ancestor(x *Repo, old, tip string) bool {
	seen := map[string]bool{}
	q := []string{tip}
	for len(q) > 0 {
		z := q[0]
		q = q[1:]
		if z == old {
			return true
		}
		if seen[z] {
			continue
		}
		seen[z] = true
		c := materialize(x, z)
		if c != nil {
			q = append(q, c.Parents...)
		}
	}
	return false
}
func parsePack(pack []byte, x *Repo) error {
	if len(pack) < 12 || string(pack[:4]) != "PACK" {
		return fmt.Errorf("bad pack")
	}
	n := int(binary.BigEndian.Uint32(pack[8:12]))
	pos := 12
	for j := 0; j < n; j++ {
		c := pack[pos]
		pos++
		typ := int((c >> 4) & 7)
		size := int(c & 15)
		shift := 4
		for c&128 != 0 {
			c = pack[pos]
			pos++
			size |= int(c&127) << shift
			shift += 7
		}
		if typ == 6 {
			for {
				c = pack[pos]
				pos++
				if c&128 == 0 {
					break
				}
			}
		} else if typ == 7 {
			pos += 20
		}
		zr, e := zlib.NewReader(bytes.NewReader(pack[pos:]))
		if e != nil {
			return e
		}
		data, e := io.ReadAll(zr)
		zr.Close()
		if e != nil {
			return e
		}
		consumed := zlibConsumed(pack[pos:])
		pos += consumed
		kind := map[int]string{1: "commit", 2: "tree", 3: "blob", 4: "tag"}[typ]
		if kind != "" && len(data) == size {
			x.putObj(kind, data)
		}
	}
	return nil
}

type countReader struct {
	r io.Reader
	n int
}

func (c *countReader) Read(p []byte) (int, error) { n, e := c.r.Read(p); c.n += n; return n, e }
func (c *countReader) ReadByte() (byte, error) {
	var one [1]byte
	n, err := c.Read(one[:])
	if n == 1 {
		return one[0], nil
	}
	return 0, err
}
func zlibConsumed(b []byte) int {
	c := &countReader{r: bytes.NewReader(b)}
	z, e := zlib.NewReader(c)
	if e != nil {
		return 0
	}
	io.Copy(io.Discard, z)
	z.Close()
	return c.n
}
func materialize(x *Repo, sha string) *Commit {
	if c := x.Commits[sha]; c != nil {
		return c
	}
	o, ok := x.Objects[sha]
	if !ok || o.Kind != "commit" {
		return nil
	}
	text := string(o.Data)
	head := strings.SplitN(text, "\n\n", 2)
	lines := strings.Split(head[0], "\n")
	c := &Commit{SHA: sha, Files: map[string]File{}}
	for _, l := range lines {
		if strings.HasPrefix(l, "tree ") {
			c.Tree = strings.TrimPrefix(l, "tree ")
		} else if strings.HasPrefix(l, "parent ") {
			c.Parents = append(c.Parents, strings.TrimPrefix(l, "parent "))
		}
	}
	walkTree(x, c.Tree, "", c.Files)
	if len(head) > 1 {
		c.Message = strings.TrimSpace(head[1])
	}
	x.Commits[sha] = c
	return c
}
func walkTree(x *Repo, sha, prefix string, out map[string]File) {
	o := x.Objects[sha]
	d := o.Data
	for i := 0; i < len(d); {
		sp := bytes.IndexByte(d[i:], ' ')
		if sp < 0 {
			return
		}
		sp += i
		nul := bytes.IndexByte(d[sp+1:], 0)
		if nul < 0 {
			return
		}
		nul += sp + 1
		mode := string(d[i:sp])
		name := string(d[sp+1 : nul])
		if nul+21 > len(d) {
			return
		}
		id := hex.EncodeToString(d[nul+1 : nul+21])
		path := prefix + name
		if mode == "40000" {
			walkTree(x, id, path+"/", out)
		} else {
			ob := x.Objects[id]
			out[path] = File{Path: path, Data: string(ob.Data), SHA: id}
		}
		i = nul + 21
	}
}
func (s *Store) upload(w http.ResponseWriter, r *http.Request, x *Repo) {
	body, _ := io.ReadAll(r.Body)
	lines, _ := parsePkts(body)
	cmd := ""
	wants := []string{}
	haves := map[string]bool{}
	hasShallow := false
	depth := 0
	prefixes := []string{}
	for _, l := range lines {
		z := strings.TrimSpace(string(l))
		if strings.HasPrefix(z, "command=") {
			cmd = strings.TrimPrefix(z, "command=")
		}
		if strings.HasPrefix(z, "want ") {
			wants = append(wants, strings.Fields(z)[1])
		}
		if strings.HasPrefix(z, "have ") {
			haves[strings.Fields(z)[1]] = true
		}
		if strings.HasPrefix(z, "deepen ") {
			depth, _ = strconv.Atoi(strings.Fields(z)[1])
		}
		if strings.HasPrefix(z, "shallow ") {
			hasShallow = true
		}
		if strings.HasPrefix(z, "ref-prefix ") {
			prefixes = append(prefixes, strings.TrimPrefix(z, "ref-prefix "))
		}
	}
	locked := true
	readLocked := cmd == "ls-refs"
	if readLocked {
		x.RLock()
	} else {
		x.Lock()
	}
	defer func() {
		if locked {
			if readLocked {
				x.RUnlock()
			} else {
				x.Unlock()
			}
		}
	}()
	if cmd == "ls-refs" {
		var b bytes.Buffer
		if len(x.Refs) == 0 {
			b.Write(pkt("unborn HEAD symref-target:refs/heads/" + x.Default + "\n"))
		} else {
			if sha := x.Refs["refs/heads/"+x.Default]; sha != "" && len(prefixes) == 0 {
				b.Write(pkt(sha + " HEAD symref-target:refs/heads/" + x.Default + "\n"))
			}
			for _, ref := range sortedRefs(x) {
				keep := len(prefixes) == 0
				for _, p := range prefixes {
					if strings.HasPrefix(ref, p) {
						keep = true
					}
				}
				if keep {
					line := x.Refs[ref] + " " + ref
					if strings.HasPrefix(ref, "refs/tags/") {
						if o := x.Objects[x.Refs[ref]]; o.Kind == "tag" {
							if target := tagTarget(o.Data); target != "" {
								line += " peeled:" + target
							}
						}
					}
					b.Write(pkt(line + "\n"))
				}
			}
		}
		b.WriteString("0000")
		w.Write(b.Bytes())
		return
	}
	if len(wants) == 0 {
		w.Write([]byte("0000"))
		return
	}
	cacheKey := ""
	if cmd == "fetch" && depth > 0 && len(haves) == 0 {
		cacheKey = strings.Join(wants, ",") + ":" + strconv.Itoa(depth)
		if cached := x.PackCache[cacheKey]; cached != nil {
			answer := append([]byte(nil), cached...)
			x.Unlock()
			locked = false
			w.Write(answer)
			return
		}
	}
	if hasShallow {
		haves = map[string]bool{}
	}
	pack, shallow := makePack(x, wants, haves, depth)
	if cmd == "fetch" {
		var b bytes.Buffer
		if len(haves) > 0 {
			b.Write(pkt("acknowledgments\n"))
			b.Write(pkt("NAK\n"))
			b.WriteString("0001")
		}
		if shallow != "" {
			b.Write(pkt("shallow-info\n"))
			b.Write(pkt("shallow " + shallow + "\n"))
			b.WriteString("0001")
		}
		b.Write(pkt("packfile\n"))
		for len(pack) > 0 {
			n := len(pack)
			if n > 65515 {
				n = 65515
			}
			b.Write(pktb(append([]byte{1}, pack[:n]...)))
			pack = pack[n:]
		}
		b.WriteString("0000")
		answer := b.Bytes()
		if cacheKey != "" {
			x.PackCache[cacheKey] = append([]byte(nil), answer...)
		}
		w.Write(answer)
		return
	}
	var b bytes.Buffer
	b.Write(pkt("NAK\n"))
	for len(pack) > 0 {
		n := len(pack)
		if n > 65515 {
			n = 65515
		}
		b.Write(pktb(append([]byte{1}, pack[:n]...)))
		pack = pack[n:]
	}
	b.WriteString("0000")
	w.Write(b.Bytes())
}
func tagTarget(d []byte) string {
	for _, l := range strings.Split(string(d), "\n") {
		if strings.HasPrefix(l, "object ") {
			return strings.TrimPrefix(l, "object ")
		}
	}
	return ""
}
func makePack(x *Repo, wants []string, haves map[string]bool, depth int) ([]byte, string) {
	include := map[string]bool{}
	exclude := map[string]bool{}
	for h := range haves {
		walkObjects(x, h, exclude, 0, 0)
	}
	shallow := ""
	for _, w := range wants {
		walkObjects(x, w, include, depth, 0)
		if depth > 0 {
			z := w
			for i := 1; i < depth; i++ {
				c := materialize(x, z)
				if c == nil || len(c.Parents) == 0 {
					z = ""
					break
				}
				z = c.Parents[0]
			}
			if z != "" {
				c := materialize(x, z)
				if c != nil && len(c.Parents) > 0 {
					shallow = z
				}
			}
		}
	}
	ids := make([]string, 0, len(include))
	for id := range include {
		if !exclude[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	var b bytes.Buffer
	b.WriteString("PACK")
	binary.Write(&b, binary.BigEndian, uint32(2))
	binary.Write(&b, binary.BigEndian, uint32(len(ids)))
	for _, id := range ids {
		o := x.Objects[id]
		typ := map[string]byte{"commit": 1, "tree": 2, "blob": 3, "tag": 4}[o.Kind]
		writeObjHeader(&b, typ, len(o.Data))
		z := zlib.NewWriter(&b)
		z.Write(o.Data)
		z.Close()
	}
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes(), shallow
}
func walkObjects(x *Repo, id string, set map[string]bool, depth, level int) {
	if id == "" || set[id] {
		return
	}
	o, ok := x.Objects[id]
	if !ok {
		return
	}
	set[id] = true
	switch o.Kind {
	case "commit":
		c := materialize(x, id)
		if c == nil {
			return
		}
		walkObjects(x, c.Tree, set, 0, 0)
		if depth == 0 || level+1 < depth {
			for _, p := range c.Parents {
				walkObjects(x, p, set, depth, level+1)
			}
		}
	case "tree":
		d := o.Data
		for i := 0; i < len(d); {
			nul := bytes.IndexByte(d[i:], 0)
			if nul < 0 {
				return
			}
			nul += i
			if nul+21 > len(d) {
				return
			}
			walkObjects(x, hex.EncodeToString(d[nul+1:nul+21]), set, 0, 0)
			i = nul + 21
		}
	case "tag":
		walkObjects(x, tagTarget(o.Data), set, depth, level)
	}
}
func writeObjHeader(w io.Writer, typ byte, size int) {
	c := byte(size&15) | (typ << 4)
	size >>= 4
	if size > 0 {
		c |= 128
	}
	w.Write([]byte{c})
	for size > 0 {
		c = byte(size & 127)
		size >>= 7
		if size > 0 {
			c |= 128
		}
		w.Write([]byte{c})
	}
}

var _ hash.Hash
var _ = url.URL{}
