*** Settings ***
Documentation    我的第一个Robot Framework测试用例

*** Test Cases ***
case 1
    ${myChar}    Set Variable    Hello Robot Framework
    Log    ${myChar}
    Should Be Equal As Strings    ${myChar}    Hello Robot Framework