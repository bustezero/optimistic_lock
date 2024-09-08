for ($i = 1; $i -le 5; $i++) {
    Start-Job -ScriptBlock {
        & "C:\Users\buste\RustroverProjects\optimistic_lock\target\debug\optimistic_lock.exe" $using:i > "C:\Users\buste\RustroverProjects\optimistic_lock\target\debug\${using:i}.txt" 2>&1
    }
}
