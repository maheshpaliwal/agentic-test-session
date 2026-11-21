package core

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestSensitiveKeyMatching(t *testing.T) {
	sens := []string{
		// as before: exact, punctuation-insensitive, long-key suffix
		"password", "Password", "PASSWD", "api_key", "API-Key", "apikey", "Card Number", "cardnumber",
		"access_token", "useraccesstoken", "Authorization", "Set-Cookie", "ssn", "x-api-key",
		// nested form and dotted names
		"user[password]", "user[credentials][token]", "card.cvv", "user.pwd", "id.ssn", "auth.password",
		// a sensitive word that is not the last one
		"password_confirmation", "password1", "password2", "currentPasswordValue", "secret_key_base", "token_id",
		"user_ssn", "APIKeyHeader",
		// headers and fields that used to slip through
		"X-Auth-Key", "X-Session-Id", "X-Goog-Iap-Jwt-Assertion", "jwt", "signature", "session_id", "otp", "passphrase",
	}
	for _, k := range sens {
		if !IsSensitiveKey(k) {
			t.Errorf("%q must be sensitive", k)
		}
	}
	plain := []string{
		"classname", "tokenizer", "max_tokens", "total_tokens", "secretary", "passwordless_login_enabled_at_all"[:12], // "passwordless"
		"author", "oauth_provider", "authority", "keyboard", "status_code", "key", "code", "state", "id", "user", "",
		"description", "assigned", "sessions", KeySession, "X-TSession-Id", KeyTraceparent, KeyTSessionTraceparent,
	}
	for _, k := range plain {
		if IsSensitiveKey(k) {
			t.Errorf("%q must not be sensitive", k)
		}
	}
}

func TestKeyWords(t *testing.T) {
	for in, want := range map[string]string{
		"user[password]":       "user password",
		"currentPasswordValue": "current password value",
		"APIKeyHeader":         "api key header",
		"x-api-key":            "x api key",
		"password2":            "password",
		"card.cvv":             "card cvv",
		"a__b  c":              "a b c",
		"HTTP2Token":           "http token",
		"123":                  "",
		"":                     "",
		"ключ_пароль":          "ключ пароль",
	} {
		if got := strings.Join(keyWords(in), " "); got != want {
			t.Errorf("keyWords(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactQueryParamOnlyKeys(t *testing.T) {
	in := "key=AIzaSECRET&code=oauth-code&sig=deadbeef&signature=s1&auth=a1&jwt=j1&session_id=sid1&pin=1234&page=2&state=ACTIVE&status_code=200"
	out := RedactQuery(in)
	for _, leak := range []string{"AIzaSECRET", "oauth-code", "deadbeef", "s1", "a1", "j1", "sid1", "1234"} {
		if strings.Contains(out, "="+leak) {
			t.Errorf("leaked %q: %s", leak, out)
		}
	}
	for _, keep := range []string{"page=2", "state=ACTIVE", "status_code=200"} {
		if !strings.Contains(out, keep) {
			t.Errorf("lost %q: %s", keep, out)
		}
	}
	// The same generic names stay readable as JSON fields.
	if out, red := RedactJSON([]byte(`{"key":"user:1","code":"PROMO10","pin":"x"}`)); red {
		t.Errorf("generic names masked outside a query string: %s", out)
	}
	// Nested and numbered form fields.
	form := RedactQuery("user%5Bpassword%5D=hunter2&password1=hunter2&password_confirmation=hunter2&name=bob")
	if strings.Contains(form, "hunter2") || !strings.Contains(form, "name=bob") {
		t.Errorf("form body: %s", form)
	}
}

func TestRedactTextHardCases(t *testing.T) {
	for name, tc := range map[string]struct{ in, leak, keep string }{
		"escaped quote":     {`{"password":"ab\"hunter2","user":"bob"`, "hunter2", `"user":"bob"`},
		"single quoted":     {`password='it\'s hunter2' AND id=7`, "hunter2", "id=7"},
		"nested form":       {`user[password]=hunter2&x=1`, "hunter2", "x=1"},
		"xml element":       {`<user><name>bob</name><password>hunter2</password></user>`, "hunter2", "<name>bob</name>"},
		"xml with attrs":    {`<ns:Secret xmlns:ns="urn:x" kind="a">hunter2</ns:Secret><b>ok</b>`, "hunter2", "<b>ok</b>"},
		"bearer":            {`Authorization failed for Bearer abc.def.ghi today`, "abc.def.ghi", "today"},
		"unterminated":      {`{"token":"hunter2`, "hunter2", `"token":`},
		"confirmation word": {`password_confirmation=hunter2;path=/`, "hunter2", "path=/"},
	} {
		out := RedactText(tc.in)
		if strings.Contains(out, tc.leak) {
			t.Errorf("%s: leaked %q in %q", name, tc.leak, out)
		}
		if !strings.Contains(out, tc.keep) {
			t.Errorf("%s: lost %q in %q", name, tc.keep, out)
		}
	}
	for _, same := range []string{"", "plain text", `{"a":"b \"c\""}`, "<a>1</a><b>2</b>", "SELECT * FROM t WHERE id = 1"} {
		if got := RedactText(same); got != same {
			t.Errorf("RedactText changed %q to %q", same, got)
		}
	}
}

func TestRedactHeadersWordMatch(t *testing.T) {
	h := http.Header{"X-Auth-Key": {"k"}, "X-Session-Id": {"s"}, "X-Goog-Iap-Jwt-Assertion": {"j"},
		"X-Tsession-Id": {"ts_1"}, "Content-Type": {"application/json"}}
	m := RedactHeaders(h)
	for _, k := range []string{"X-Auth-Key", "X-Session-Id", "X-Goog-Iap-Jwt-Assertion"} {
		if m[k][0] != RedactedValue {
			t.Errorf("%s not masked: %v", k, m[k])
		}
	}
	if m["X-Tsession-Id"][0] != "ts_1" || m["Content-Type"][0] != "application/json" {
		t.Errorf("over-masked: %v", m)
	}
}

// The verdict memo must follow SetRedactKeys and must not grow with data keys.
func TestSensitiveCacheBoundedAndReset(t *testing.T) {
	defer SetRedactKeys(DefaultRedactKeys)
	SetRedactKeys([]string{"foo"})
	if !IsSensitiveKey("foo_bar") || IsSensitiveKey("password") {
		panic("custom list not applied")
	}
	for i := range 3 * maxKeyCache {
		IsSensitiveKey(fmt.Sprintf("user-%d", i))
	}
	if n := matcher.Load().cached.Load(); n > maxKeyCache {
		panic(fmt.Sprintf("memo grew to %d entries", n))
	}
	if !IsSensitiveKey("late_foo") { // past the cap verdicts are still computed
		panic("uncached key misjudged")
	}
	SetRedactKeys(DefaultRedactKeys)
	if IsSensitiveKey("foo_bar") || !IsSensitiveKey("password") {
		panic("stale verdicts survived SetRedactKeys")
	}
}

func BenchmarkSensitiveKey(b *testing.B) {
	keys := []string{"id", "customer_name", "password", "createdAt", "access_token", "items", "status_code", "user[password]"}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		IsSensitiveKey(keys[i%len(keys)])
	}
}
