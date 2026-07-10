// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package component

import (
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/lonng/nano/session"
)

var (
	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfBytes   = reflect.TypeOf(([]byte)(nil))
	typeOfSession = reflect.TypeOf(session.New(nil))
	typeOfResponder = reflect.TypeOf(&session.Responder{})
)

func isExported(name string) bool {
	w, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(w)
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// isHandlerMethod decide a method is suitable handler method
func isHandlerMethod(method reflect.Method) bool {
	mt := method.Type
	// Method must be exported.
	if method.PkgPath != "" {
		return false
	}

	// Method needs either:
	// 1) receiver, *Session, []byte|*Req
	// 2) receiver, *Session, []byte|*Req, mid
	if mt.NumIn() != 3 && mt.NumIn() != 4 {
		return false
	}

	//一个返回值
	// Method needs one outs: error
	if mt.NumOut() != 1 {
		return false
	}

	//校验session参数
	if t1 := mt.In(1); t1.Kind() != reflect.Ptr || t1 != typeOfSession {
		return false
	}

	//校验业务参数
	if (mt.In(2).Kind() != reflect.Ptr && mt.In(2) != typeOfBytes) || mt.Out(0) != typeOfError {
		return false
	}

	// Optional 4th argument: legacy mid or responder
	if mt.NumIn() == 4 {
		t4 := mt.In(3)
		if t4.Kind() == reflect.Uint64 {
			return true
		}
		if t4 == typeOfResponder {
			return true
		}
		return false
	}

	return true
}
