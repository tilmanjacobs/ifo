tell application "Terminal"
    activate
    do script "cd \"" & (POSIX path of (path to me as text)) & "/..\"; ./run_ocr.sh"
end tell 