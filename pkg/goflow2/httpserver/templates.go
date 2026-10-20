package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

// templateFieldView 是单个字段的展示视图，在原始字段之上补充英文名与中文释义。
type templateFieldView struct {
	Index  int    `json:"index"`
	Type   uint16 `json:"type"`
	Name   string `json:"name"`
	Zh     string `json:"zh"`
	Length uint16 `json:"length"`
	Pen    uint32 `json:"pen"`
}

// templateView 是单个模板的展示视图。
type templateView struct {
	Key        string              `json:"key"`
	Version    uint16              `json:"version"`
	ObsDomain  uint32              `json:"obsDomainId"`
	TemplateID uint16              `json:"templateId"`
	FieldCount uint16              `json:"fieldCount"`
	Fields     []templateFieldView `json:"fields"`
}

// routerTemplates 是按设备（router）分组的模板集合。
type routerTemplates struct {
	Router    string         `json:"router"`
	IP        string         `json:"ip"`
	Port      string         `json:"port"`
	Templates []templateView `json:"templates"`
}

// templateBrowserDoc 是模板浏览接口的响应体。
type templateBrowserDoc struct {
	Routers []routerTemplates `json:"routers"`
}

// TemplateSource supplies the live template store for the browser endpoint.
// It is satisfied by the persistence manager.
type TemplateSource interface {
	// GetAll returns templates keyed by router, then by a packed
	// version/observation-domain/template-id key.
	GetAll() map[string]netflow.FlowBaseTemplateSet
}

// splitRouter 拆开形如 "117.185.127.2:2055" 的 router key。
// 缺少端口时 ip 为空、port 承载原值，便于前端筛选时两种写法都能命中。
func splitRouter(router string) (ip string, port string) {
	if idx := strings.LastIndex(router, ":"); idx >= 0 {
		return router[:idx], router[idx+1:]
	}
	return "", router
}

// matchesRouter 判断某个 router key 是否命中查询条件。
// 条件为空时匹配全部；否则对 IP 与「IP:端口」两种形态都做前缀匹配，
// 这样 ?router=117.185.127.2 能同时命中 117.185.127.2:2055 等所有端口。
func matchesRouter(router, filter string) bool {
	if filter == "" {
		return true
	}
	router = strings.TrimSpace(router)
	if router == "" {
		return false
	}
	return strings.HasPrefix(router, filter) || strings.HasPrefix(filter, router)
}

// asTemplateRecord 从模板存储里取出统一的模板记录。
//
// 运行时解码路径（netflow.DecodeTemplateSet）与持久化预加载路径
// （templates.decodeTemplatePayload）存入的都是 TemplateRecord 值，
// 但接口本身是 interface{}，故同时兼容指针形态。
func asTemplateRecord(raw interface{}) (*netflow.TemplateRecord, bool) {
	switch t := raw.(type) {
	case netflow.TemplateRecord:
		return &t, true
	case *netflow.TemplateRecord:
		if t == nil {
			return nil, false
		}
		return t, true
	default:
		// 选项模板（NFv9OptionsTemplateRecord / IPFIXOptionsTemplateRecord）
		// 结构不同且不含 Fields，此处跳过
		return nil, false
	}
}

// buildTemplateBrowserDoc 把模板存储转换为按设备分组的展示结构。
// routerFilter 为空表示不过滤；结果按设备名与模板键排序，保证前端渲染顺序稳定。
func buildTemplateBrowserDoc(store TemplateSource, routerFilter string) templateBrowserDoc {
	doc := templateBrowserDoc{Routers: []routerTemplates{}}
	if store == nil {
		return doc
	}

	all := store.GetAll()
	for router, set := range all {
		if len(set) == 0 || !matchesRouter(router, routerFilter) {
			continue
		}
		ip, port := splitRouter(router)
		entry := routerTemplates{Router: router, IP: ip, Port: port, Templates: []templateView{}}

		for packed, raw := range set {
			record, ok := asTemplateRecord(raw)
			if !ok {
				continue
			}
			version := uint16(packed >> 48)
			obsDomain := uint32(packed >> 16)
			templateID := uint16(packed)

			view := templateView{
				Key:        strconv.Itoa(int(version)) + "/" + strconv.FormatUint(uint64(obsDomain), 10) + "/" + strconv.Itoa(int(templateID)),
				Version:    version,
				ObsDomain:  obsDomain,
				TemplateID: templateID,
				FieldCount: record.FieldCount,
				Fields:     make([]templateFieldView, 0, len(record.Fields)),
			}
			for i, f := range record.Fields {
				name, zh := fieldDescription(version, f.Type)
				view.Fields = append(view.Fields, templateFieldView{
					Index:  i,
					Type:   f.Type,
					Name:   name,
					Zh:     zh,
					Length: f.Length,
					Pen:    f.Pen,
				})
			}
			entry.Templates = append(entry.Templates, view)
		}

		sort.Slice(entry.Templates, func(i, j int) bool {
			a, b := entry.Templates[i], entry.Templates[j]
			if a.Version != b.Version {
				return a.Version < b.Version
			}
			if a.ObsDomain != b.ObsDomain {
				return a.ObsDomain < b.ObsDomain
			}
			return a.TemplateID < b.TemplateID
		})
		doc.Routers = append(doc.Routers, entry)
	}

	sort.Slice(doc.Routers, func(i, j int) bool { return doc.Routers[i].Router < doc.Routers[j].Router })
	return doc
}

// TemplateHandler returns a handler serving templates grouped by device, with
// each field annotated by its standard English name and a Chinese description.
//
// It supports an optional "router" query parameter to narrow the result down to
// a single device; the value is matched as a prefix so that an IP such as
// "10.1.2.3" selects every template exported by that address regardless of the
// source port.
func TemplateHandler(store TemplateSource) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		if store == nil {
			wr.WriteHeader(http.StatusNotFound)
			if _, err := wr.Write([]byte("Not Found\n")); err != nil {
				slog.Error("error writing HTTP", slog.String("error", err.Error()))
			}
			return
		}
		doc := buildTemplateBrowserDoc(store, strings.TrimSpace(r.URL.Query().Get("router")))
		body, err := json.Marshal(doc)
		if err != nil {
			wr.WriteHeader(http.StatusInternalServerError)
			if _, werr := wr.Write([]byte("Internal Server Error\n")); werr != nil {
				slog.Error("error writing HTTP", slog.String("error", werr.Error()))
			}
			return
		}
		wr.Header().Add("Content-Type", "application/json")
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write(body); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))
		}
	}
}
