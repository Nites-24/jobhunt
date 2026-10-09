<#
.SYNOPSIS
    Purges sensitive personal identity details and PII from git commit history.

.DESCRIPTION
    Uses git-filter-repo to rewrite git history, replacing sensitive candidate
    names, email addresses, personal URLs, and resume filenames with identity-agnostic
    placeholders ("Alex Hunter", "candidate@example.com", "resume.pdf").

.PARAMETER Force
    Skips the confirmation prompt and proceeds with rewriting.

.PARAMETER DryRun
    Generates the replacement rules and displays the plan without modifying git history.

.EXAMPLE
    .\tools\ops\purge_git_history.ps1 -DryRun
    .\tools\ops\purge_git_history.ps1 -Force
#>

[CmdletBinding()]
param(
    [switch]$Force,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  JOBHUNT GIT HISTORY IDENTITY PURGE TOOL" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Ensure we are in the repo root
$repoRoot = (git rev-parse --show-toplevel 2>$null)
if (-not $repoRoot) {
    Write-Error "Error: Must be run from within a Git repository."
    exit 1
}
Set-Location $repoRoot

# 2. Check working tree status
$status = (git status --porcelain)
if ($status -and -not $Force -and -not $DryRun) {
    Write-Warning "Working tree has uncommitted changes. Please commit or stash them first, or use -Force."
    Write-Host "Uncommitted changes:" -ForegroundColor Yellow
    git status -s
    exit 1
}

# 3. Load identity scrub expressions from untracked rules file
$rulesFile = if ($args[0]) { $args[0] } else { ".purge_rules.txt" }
if (-not (Test-Path $rulesFile)) {
    Write-Host "Creating template rules file: $rulesFile..." -ForegroundColor Yellow
    @(
        "# Format: regex:pattern_to_match==>replacement_text",
        "regex:Old Name==>Alex Hunter",
        "regex:oldemail@example\.com==>candidate@example.com",
        "regex:Old_Resume\.pdf==>resume.pdf"
    ) | Out-File -FilePath $rulesFile -Encoding utf8
    Write-Warning "Please edit '$rulesFile' with your target replacement rules, then rerun this script."
    exit 1
}

$replacements = Get-Content $rulesFile | Where-Object { $_ -and -not $_.StartsWith("#") }
if (-not $replacements) {
    Write-Error "No valid replacement rules found in '$rulesFile'."
    exit 1
}

Write-Host "`nReplacement Rules to apply from '$rulesFile':" -ForegroundColor Green
foreach ($rule in $replacements) {
    Write-Host "  * $rule" -ForegroundColor Gray
}

if ($DryRun) {
    Write-Host "`n[DryRun] Verification complete. No changes made to Git history." -ForegroundColor Yellow
    exit 0
}

# 4. Check for git-filter-repo
$hasFilterRepo = (Get-Command "git-filter-repo" -ErrorAction SilentlyContinue)
if (-not $hasFilterRepo) {
    # Check if python -m git_filter_repo is available
    $pyCheck = python -c "import git_filter_repo" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`ngit-filter-repo is not installed. Installing via pip..." -ForegroundColor Yellow
        python -m pip install git-filter-repo
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install git-filter-repo. Please run 'pip install git-filter-repo' manually."
            exit 1
        }
    }
}

# 5. Create backup branch before rewrite
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupBranch = "backup-pre-purge-$timestamp"
Write-Host "`nCreating backup branch: $backupBranch" -ForegroundColor Cyan
git branch $backupBranch
Write-Host "Backup branch created successfully." -ForegroundColor Green

# 6. Write replacement expressions to temporary file
$tempFile = [System.IO.Path]::GetTempFileName()
try {
    $replacements | Out-File -FilePath $tempFile -Encoding utf8

    Write-Host "`nExecuting git-filter-repo..." -ForegroundColor Cyan
    if ($hasFilterRepo) {
        git-filter-repo --replace-text $tempFile --force
    } else {
        python -m git_filter_repo --replace-text $tempFile --force
    }

    if ($LASTEXITCODE -eq 0) {
        Write-Host "`nGit history rewrite SUCCESSFUL!" -ForegroundColor Green
        Write-Host "=================================================================" -ForegroundColor Cyan
        Write-Host "  NEXT STEPS:" -ForegroundColor Yellow
        Write-Host "  1. Run audit to verify 0 leaks:" -ForegroundColor White
        Write-Host "     python tools/ops/run_agents.py --security" -ForegroundColor Gray
        Write-Host "  2. If satisfied, force-push to remote:" -ForegroundColor White
        Write-Host "     git push origin main --force" -ForegroundColor Gray
        Write-Host "  3. Backup branch is preserved at: $backupBranch" -ForegroundColor White
        Write-Host "=================================================================" -ForegroundColor Cyan
    } else {
        Write-Error "git-filter-repo execution failed."
        exit 1
    }
} finally {
    if (Test-Path $tempFile) {
        Remove-Item -Path $tempFile -Force -ErrorAction SilentlyContinue
    }
}
