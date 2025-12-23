# Scraper ATIH - Version optimisée avec parallélisation
# Usage: source("parallel_scraper.R")
# 
# Améliorations principales:
# - Parallélisation avec future/furrr
# - Traitement par niveau (breadth-first) pour maximiser la parallélisation
# - Cache HTML optionnel
# - Retry logic avec backoff exponentiel
# - Vérification de fichiers avant requête quand possible

# Charger les dépendances
library(rvest)
library(httr)
library(dplyr)
library(stringr)
library(purrr)
library(future)
library(furrr)
library(digest)

# Configuration
BASE_URL <- "https://www.scansante.fr/applications/statistiques-activite-MCO-par-diagnostique-et-actes/submit"
OUTPUT_DIR <- "data/atih_scraped3"
HTML_CACHE_DIR <- "data/html_cache"
LINKS_CACHE_DIR <- "data/links_cache"

# Créer les dossiers de sortie
if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}
if (!dir.exists(HTML_CACHE_DIR)) {
  dir.create(HTML_CACHE_DIR, recursive = TRUE)
}
if (!dir.exists(LINKS_CACHE_DIR)) {
  dir.create(LINKS_CACHE_DIR, recursive = TRUE)
}

# Fonction optimisée pour extraire les données
extract_data <- function(html) {
  tables <- html %>% html_nodes("table")
  if (length(tables) == 0) {
    return(NULL)
  }

  # Prendre le plus grand tableau
  table_sizes <- map_int(tables, ~ length(html_nodes(., "tr")))
  main_table <- tables[which.max(table_sizes)]

  # Essayer html_table() d'abord (plus rapide)
  tryCatch({
    df <- html_table(main_table, fill = TRUE, header = TRUE)
    if (!is.null(df) && nrow(df) > 0) {
      return(df)
    }
  }, error = function(e) {
    # Fallback sur l'ancienne méthode
  })

  # Méthode de fallback
  rows <- main_table %>%
    html_nodes("tr") %>%
    map(~ html_nodes(., "td, th") %>% html_text(trim = TRUE)) %>%
    compact()

  if (length(rows) < 2) {
    return(NULL)
  }

  df <- do.call(rbind, rows) %>% as.data.frame(stringsAsFactors = FALSE)
  colnames(df) <- df[1, ]
  df <- df[-1, ]
  return(df)
}

# Fonction pour extraire les liens
extract_links <- function(html) {
  html %>%
    html_nodes("a[href*='submit']") %>%
    html_attr("href") %>%
    .[!is.na(.)]
}

# Fonction pour construire l'URL
build_url <- function(params) {
  paste0(
    BASE_URL,
    "?",
    "snatnav=&annee=",
    params$annee,
    "&type_cat_etab=",
    params$type_cat_etab,
    "&type_code=",
    params$type_code,
    "&code=",
    params$code,
    "&niveau=",
    params$niveau,
    "&codh=",
    params$codh
  )
}

# Fonction pour générer le nom de fichier
generate_filename <- function(params, level) {
  paste0(
    "niveau_",
    level,
    "_code_",
    params$code,
    "_codh_",
    params$codh,
    ".csv"
  )
}

# Fonction pour obtenir le chemin du cache de liens
get_links_cache_path <- function(params, level) {
  cache_key <- paste0("niveau_", level, "_codh_", params$codh, ".rds")
  file.path(LINKS_CACHE_DIR, cache_key)
}

# Fonction pour sauvegarder les liens
save_links <- function(params, level, links) {
  if (!is.null(links) && length(links) > 0) {
    cache_path <- get_links_cache_path(params, level)
    tryCatch({
      saveRDS(links, cache_path)
    }, error = function(e) {
      # Ignorer les erreurs de sauvegarde
    })
  }
}

# Fonction pour charger les liens depuis le cache
load_links <- function(params, level) {
  cache_path <- get_links_cache_path(params, level)
  if (file.exists(cache_path)) {
    tryCatch({
      return(readRDS(cache_path))
    }, error = function(e) {
      # Si le cache est corrompu, retourner NULL
      return(NULL)
    })
  }
  return(NULL)
}

# Fonction avec retry logic
fetch_with_retry <- function(url, max_retries = 3, sleep_time = 0.01) {
  for (attempt in 1:max_retries) {
    tryCatch({
      Sys.sleep(sleep_time)
      response <- GET(url, user_agent("Mozilla/5.0"), timeout(30))
      
      if (status_code(response) == 200) {
        return(response)
      } else if (status_code(response) == 429) {
        # Rate limiting - attendre plus longtemps
        wait_time <- sleep_time * (2 ^ attempt)
        cat("  -> Rate limited, waiting", wait_time, "s\n")
        Sys.sleep(wait_time)
      } else {
        cat("  -> HTTP error:", status_code(response), "\n")
        if (attempt < max_retries) {
          Sys.sleep(sleep_time * (2 ^ attempt))  # Backoff exponentiel
        }
      }
    }, error = function(e) {
      cat("  -> Error:", conditionMessage(e), "\n")
      if (attempt < max_retries) {
        Sys.sleep(sleep_time * (2 ^ attempt))
      }
    })
  }
  return(NULL)
}

# Fonction pour obtenir ou récupérer le HTML (avec cache)
get_or_fetch_html <- function(url, params, use_cache = TRUE, overwrite = FALSE) {
  cache_file <- NULL
  
  if (use_cache) {
    cache_key <- digest::digest(url)
    cache_file <- file.path(HTML_CACHE_DIR, paste0(cache_key, ".html"))
    
    if (file.exists(cache_file) && !overwrite) {
      tryCatch({
        return(read_html(cache_file))
      }, error = function(e) {
        # Si le cache est corrompu, continuer avec la requête
      })
    }
  }
  
  response <- fetch_with_retry(url)
  if (is.null(response)) {
    return(NULL)
  }
  
  html <- read_html(response)
  
  # Sauvegarder en cache si activé
  if (use_cache && !is.null(cache_file)) {
    tryCatch({
      write_html(html, cache_file)
    }, error = function(e) {
      # Ignorer les erreurs de cache
    })
  }
  
  return(html)
}

# Fonction pour scraper une URL unique
scrape_single_url <- function(params, level, sleep_time = 0.01, 
                              use_cache = TRUE, overwrite = FALSE) {
  url <- build_url(params)
  filename <- generate_filename(params, level)
  filepath <- file.path(OUTPUT_DIR, filename)
  
  # Vérifier si le fichier existe déjà
  file_exists <- file.exists(filepath) && !overwrite
  
  # Si le fichier existe, essayer de charger les liens depuis le cache
  if (file_exists) {
    cached_links <- load_links(params, level)
    if (!is.null(cached_links)) {
      # On a déjà les liens en cache, pas besoin de requête HTTP
      return(list(data = NULL, links = cached_links, params = params, level = level))
    }
    # Si pas de cache de liens, on doit récupérer le HTML (mais via cache HTML si disponible)
  }
  
  # Récupérer le HTML (utilisera le cache HTML si disponible)
  html <- get_or_fetch_html(url, params, use_cache, overwrite)
  if (is.null(html)) {
    return(list(data = NULL, links = NULL, params = params, level = level))
  }
  
  # Extraire les données seulement si nécessaire
  data <- NULL
  if (!file_exists) {
    data <- extract_data(html)
    
    # Sauvegarder si on a des données
    if (!is.null(data) && !is.null(nrow(data)) && nrow(data) > 0) {
      tryCatch({
        data.table::fwrite(data, filepath, row.names = FALSE, encoding = "UTF-8")
      }, error = function(e) {
        write.csv(data, filepath, row.names = FALSE, fileEncoding = "UTF-8")
      })
    }
  }
  
  # Extraire les liens
  links <- extract_links(html)
  
  # Sauvegarder les liens en cache pour éviter les futures requêtes
  save_links(params, level, links)
  
  return(list(data = data, links = links, params = params, level = level))
}

# Fonction pour extraire les paramètres d'un lien
extract_params_from_link <- function(link, level, base_params) {
  if (!grepl("codh=([0-9]+)", link)) {
    return(NULL)
  }
  
  codh <- gsub(".*codh=([0-9]+).*", "\\1", link)
  
  return(list(
    code = "",
    codh = codh,
    niveau = as.character(level),
    annee = base_params$annee,
    type_cat_etab = base_params$type_cat_etab,
    type_code = base_params$type_code
  ))
}

# Fonction principale optimisée avec parallélisation
scrape_atih_optimized <- function(
  max_level = 1,
  max_links_per_level = NULL,
  sleep_time = 0.01,
  overwrite = FALSE,
  use_cache = TRUE,
  n_workers = 4
) {
  cat("=== Scraper ATIH - Version optimisée ===\n")
  cat("Niveau maximum:", max_level, "\n")
  cat("Workers parallèles:", n_workers, "\n")
  cat("Dossier de sortie:", OUTPUT_DIR, "\n")
  cat("Cache HTML:", ifelse(use_cache, "activé", "désactivé"), "\n\n")
  
  # Planifier la parallélisation
  plan(multisession, workers = n_workers)
  
  # Paramètres de base
  base_params <- list(
    annee = "2024",
    type_cat_etab = "pub",
    type_code = "cim"
  )
  
  # Paramètres de départ
  start_params <- list(
    code = "",
    codh = "000000000",
    niveau = "0",
    annee = base_params$annee,
    type_cat_etab = base_params$type_cat_etab,
    type_code = base_params$type_code
  )
  
  # Traitement par niveau (breadth-first pour maximiser la parallélisation)
  current_level_params <- list(start_params)
  level <- 0
  
  while (level <= max_level && length(current_level_params) > 0) {
    cat("\n=== Niveau", level, "-", length(current_level_params), "URLs à traiter ===\n")
    
    # Limiter le nombre de liens par niveau si demandé
    if (!is.null(max_links_per_level) && length(current_level_params) > max_links_per_level) {
      current_level_params <- current_level_params[1:max_links_per_level]
      cat("  -> Limité à", max_links_per_level, "liens\n")
    }
    
    # Traiter tous les URLs de ce niveau en parallèle
    results <- future_map(
      current_level_params,
      ~ scrape_single_url(
        .x, 
        level, 
        sleep_time = sleep_time / n_workers,  # Réduire le sleep par worker
        use_cache = use_cache,
        overwrite = overwrite
      ),
      .progress = TRUE,
      .options = furrr_options(seed = TRUE)
    )
    
    # Collecter les liens pour le niveau suivant
    next_level_params <- list()
    
    if (level < max_level) {
      for (result in results) {
        if (!is.null(result$links) && length(result$links) > 0) {
          for (link in result$links) {
            next_params <- extract_params_from_link(link, level + 1, base_params)
            if (!is.null(next_params)) {
              next_level_params <- c(next_level_params, list(next_params))
            }
          }
        }
      }
      
      # Dédupliquer les paramètres (même codh = même URL)
      if (length(next_level_params) > 0) {
        param_strings <- map_chr(next_level_params, ~ paste(.x$codh, .x$niveau, sep = "_"))
        unique_indices <- !duplicated(param_strings)
        next_level_params <- next_level_params[unique_indices]
      }
      
      cat("  ->", length(next_level_params), "liens uniques pour le niveau suivant\n")
    }
    
    current_level_params <- next_level_params
    level <- level + 1
  }
  
  # Nettoyer les workers
  plan(sequential)
  
  cat("\n=== Scraping terminé! ===\n")
  
  # Afficher un résumé
  files <- list.files(OUTPUT_DIR, pattern = "\\.csv$", full.names = FALSE)
  cat("Fichiers générés:", length(files), "\n")
  
  # Afficher quelques exemples
  if (length(files) > 0) {
    cat("Exemples:\n")
    for (file in head(files, 5)) {
      cat("  -", file, "\n")
    }
    if (length(files) > 5) {
      cat("  ... et", length(files) - 5, "autres\n")
    }
  }
}

# Exemple d'utilisation:
scrape_atih_optimized(
  max_level = 4,
  sleep_time = 0.01,
  overwrite = FALSE,
  use_cache = TRUE,
  n_workers = 8  # Ajuster selon votre machine
)

