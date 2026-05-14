# 陳鍾誠的寫程式專屬 skill

1. 必須要寫詳細的單元測試，還有系統測試
    * 如果是網站，必須對 server api 測試，還要使用 Playwright 對網站進行 e2e 測試。
2. 測試框架
    * python 使用 pytest
    * rust 使用 cargo test
    * 必須寫一個 test.sh 做專案測試
3. 程式規範
    * 必須經過 lint 格式檢查與自動格式化（python 使用 ruff）
    * 程式超過 1000 行，就要分成兩個檔案模組。
4. 規劃寫在 _doc/ 下，每一個版本都要寫出 vx.y.md 
    * 例如： v0.1.md v0.2.md ....v 1.1.md
    * 每次進版基本上都前進 0.1 版
5. 語法必須修改到沒有 warning 
    * 如果是 rust ，可以用 #![allow(dead_code, unused)]
    * 如果是 C 必須改到沒 warning.
6. 所有路徑都應該使用相對路徑，要跨平台能運作的 
    * 不能使用 /xxx/.... 這樣的路徑，應該使用 ../ ./ 這樣的路徑
