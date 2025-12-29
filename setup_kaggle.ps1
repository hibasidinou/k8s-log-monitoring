# setup_kaggle.ps1
param(
    [string]$kaggleUsername,
    [string]$kaggleKey,
    [string]$userProfile = $env:USERPROFILE
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Configuration Kaggle pour Windows" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Vérifier si l'utilisateur n'a pas fourni les credentials
if (-not $kaggleUsername -or -not $kaggleKey) {
    Write-Host "ℹ️  Informations Kaggle non fournies en paramètre" -ForegroundColor Yellow
    Write-Host ""
    
    # Demander les informations interactivement
    $kaggleUsername = Read-Host "Entrez votre nom d'utilisateur Kaggle"
    $kaggleKey = Read-Host "Entrez votre clé Kaggle (kaggle.json key)" -AsSecureString
    
    # Convertir le SecureString en texte clair
    $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($kaggleKey)
    $kaggleKey = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)
}

# 1. Créer le dossier .kaggle
$kaggleDir = Join-Path $userProfile ".kaggle"
if (-not (Test-Path $kaggleDir)) {
    New-Item -ItemType Directory -Path $kaggleDir -Force | Out-Null
    Write-Host "✓ Dossier créé: $kaggleDir" -ForegroundColor Green
} else {
    Write-Host "✓ Dossier existe déjà: $kaggleDir" -ForegroundColor Green
}

# 2. Créer le fichier kaggle.json
$kaggleJsonPath = Join-Path $kaggleDir "kaggle.json"

# Contenu JSON
$jsonContent = @"
{
  "username": "$kaggleUsername",
  "key": "$kaggleKey"
}
"@

# Écrire le fichier
$jsonContent | Out-File -FilePath $kaggleJsonPath -Encoding UTF8 -Force

Write-Host "✓ Fichier créé: $kaggleJsonPath" -ForegroundColor Green

# 3. Sécuriser les permissions (important sur Windows)
try {
    # Rendre le fichier accessible uniquement à l'utilisateur courant
    icacls $kaggleJsonPath /inheritance:r /grant:r "${env:USERNAME}:(R,W)" 2>$null
    Write-Host "✓ Permissions sécurisées sur le fichier" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Impossible de modifier les permissions (exécutez en admin si besoin)" -ForegroundColor Yellow
}

# 4. Tester l'installation Kaggle
Write-Host ""
Write-Host "🔧 Test de l'installation Kaggle..." -ForegroundColor Cyan

# Vérifier si kaggle est installé
try {
    $kaggleCheck = Get-Command kaggle -ErrorAction Stop
    Write-Host "✓ Kaggle CLI est installé" -ForegroundColor Green
    
    # Tester la connexion
    Write-Host "🔍 Test de connexion à Kaggle..." -ForegroundColor Cyan
    $testResult = kaggle datasets list -s "kubernetes" --max-size 1 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Connexion Kaggle réussie!" -ForegroundColor Green
        Write-Host ""
        Write-Host "📊 Résultat du test:" -ForegroundColor Cyan
        Write-Host $testResult
    } else {
        Write-Host "❌ Erreur de connexion Kaggle" -ForegroundColor Red
        Write-Host "Message d'erreur:" -ForegroundColor Red
        Write-Host $testResult
    }
    
} catch {
    Write-Host "❌ Kaggle CLI n'est pas installé" -ForegroundColor Red
    Write-Host ""
    Write-Host "📥 Installation de Kaggle..." -ForegroundColor Yellow
    
    # Installer kaggle via pip
    try {
        pip install kaggle --upgrade
        Write-Host "✓ Kaggle installé avec pip" -ForegroundColor Green
        
        # Retester après installation
        Write-Host "🔍 Nouveau test de connexion..." -ForegroundColor Cyan
        kaggle datasets list -s "kubernetes" --max-size 1
    } catch {
        Write-Host "❌ Impossible d'installer Kaggle automatiquement" -ForegroundColor Red
        Write-Host ""
        Write-Host "📝 Instructions manuelles:" -ForegroundColor Yellow
        Write-Host "1. Ouvrez CMD ou PowerShell en tant qu'administrateur" -ForegroundColor Yellow
        Write-Host "2. Exécutez: pip install kaggle --upgrade" -ForegroundColor Yellow
        Write-Host "3. Redémarrez votre terminal" -ForegroundColor Yellow
    }
}

# 5. Afficher les informations de configuration
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  RÉSUMÉ DE LA CONFIGURATION" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Dossier Kaggle: $kaggleDir" -ForegroundColor White
Write-Host "Fichier config: $kaggleJsonPath" -ForegroundColor White
Write-Host "Username: $kaggleUsername" -ForegroundColor White
Write-Host ""

# 6. Commande pour télécharger le dataset Kubernetes
Write-Host "📥 Pour télécharger le dataset Kubernetes:" -ForegroundColor Green
Write-Host "kaggle datasets download -d andrewmvd/kubernetes-log-analysis" -ForegroundColor Yellow
Write-Host "OU" -ForegroundColor Yellow
Write-Host "kaggle datasets download -d ealtman2018/kubernetes-log-dataset" -ForegroundColor Yellow
Write-Host ""
Write-Host "🔧 Pour décompresser: tar -xf kubernetes-log-analysis.zip" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan