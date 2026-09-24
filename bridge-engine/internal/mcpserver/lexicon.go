package mcpserver

// Built-in words for recognizing intents in Chinese (simplified and
// traditional). English and Thai live in intentRules; more languages can be
// added at runtime with ExtendLexicon (for example from config).
var builtinLexicon = map[string][]string{
	"greeting":      {"你好", "您好", "嗨", "哈囉", "哈喽"},
	"capabilities":  {"你能做什么", "你能做什麼", "能做什么", "能做什麼", "怎么用", "怎麼用", "帮助", "幫助", "功能"},
	"overview":      {"概况", "概況", "总览", "總覽", "状态", "狀態", "怎么样", "怎麼樣", "全部", "正常吗", "正常嗎", "健康"},
	"diagnose":      {"为什么", "為什麼", "慢", "卡住", "不工作", "坏了", "壞了", "故障", "问题", "問題", "异常", "異常", "延迟", "延遲", "积压", "積壓", "怎么了", "怎麼了"},
	"explain_error": {"错误", "錯誤", "报错", "報錯", "异常信息", "異常訊息", "失败", "失敗", "超时", "超時", "什么意思", "什麼意思", "从哪里来", "從哪裡來"},
	"events":        {"发生了什么", "發生了什麼", "最近", "昨晚", "昨天", "历史", "歷史", "日志", "日誌", "刚才", "剛才"},
	"tuning":        {"调优", "調優", "吞吐", "容量", "扩容", "擴容", "性能", "效能", "配置建议", "配置建議", "每秒", "规格", "規格", "更快"},
	"dlq":           {"死信", "拒绝", "拒絕", "失败消息", "失敗訊息"},
	"create":        {"创建", "創建", "建立", "新建", "新增管道", "新增 pipeline"},
	"change":        {"修改", "更改", "更新", "编辑", "編輯", "设置", "設置", "设定", "設定", "增加", "减少", "減少", "启用", "啟用", "禁用"},
	"pause":         {"暂停", "暫停", "停止"},
	"resume":        {"恢复", "恢復", "继续", "繼續", "重新开始", "重新開始"},
	"retry":         {"重试", "重試", "重新发送", "重新發送", "重新处理", "重新處理"},
	"config_view":   {"配置", "设定值", "設定值"},
	"topics":        {"主题", "主題", "分区", "分區"},
	"check_data":    {"数据异常", "數據異常", "资料异常", "資料異常", "格式", "脏数据", "髒數據", "数据质量", "數據質量", "参数", "參數"},
	"data_rules":    {"规则", "規則", "拦截", "攔截", "校验", "校驗", "过滤", "過濾"},
}

var builtinTimeWords = map[int][]string{
	60:      {"过去一小时", "過去一小時", "一小时", "一小時"},
	14 * 60: {"昨晚", "昨夜"},
	36 * 60: {"昨天"},
	24 * 60: {"今天"},
	12 * 60: {"今早", "今天早上"},
	15:      {"刚才", "剛才"},
}

func init() {
	for intent, words := range builtinLexicon {
		ExtendLexicon(intent, words...)
	}
	for minutes, words := range builtinTimeWords {
		timeWindows = append(timeWindows, struct {
			words   []string
			minutes int
		}{words, minutes})
	}
}

// ExtendLexicon adds words that signal an intent, for languages beyond the
// built-in ones. Unknown intents are ignored.
func ExtendLexicon(intent string, words ...string) {
	for i := range intentRules {
		if intentRules[i].intent == intent {
			intentRules[i].keywords = append(intentRules[i].keywords, words...)
			return
		}
	}
}
