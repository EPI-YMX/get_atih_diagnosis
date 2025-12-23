# Scraper ATIH - Version rapide et optimisée
# Usage: source("R/quick_scraper.R")

# Charger les dépendances
library(rvest)
library(httr)
library(dplyr)
library(stringr)
library(purrr)

# Configuration
BASE_URL <- "https://www.scansante.fr/applications/statistiques-activite-MCO-par-diagnostique-et-actes/submit"
OUTPUT_DIR <- "data/atih_scraped"

# Créer le dossier de sortie
if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

# Fonction simplifiée pour extraire les données
extract_data <- function(html) {
  tables <- html %>% html_nodes("table")
  if (length(tables) == 0) {
    return(NULL)
  }

  # Prendre le plus grand tableau
  table_sizes <- map_int(tables, ~ length(html_nodes(., "tr")))
  main_table <- tables[which.max(table_sizes)]

  # Extraire les données
  rows <- main_table %>%
    html_nodes("tr") %>%
    map(~ html_nodes(., "td, th") %>% html_text(trim = TRUE)) %>%
    compact()

  if (length(rows) < 2) {
    return(NULL)
  }

  # Créer le data frame
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

# Fonction de scraping récursif simplifiée
scrape_recursive <- function(
  params,
  level = 0,
  max_level = 2,
  max_links_per_level = NULL,
  sleep_time,
  overwrite = FALSE
) {
  if (level > max_level) {
    return()
  }

  # Construire l'URL
  url <- paste0(
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

  cat("Niveau", level, "- Scraping:", basename(url), "\n")

  # Faire la requête
  Sys.sleep(sleep_time) # Pause respectueuse
  response <- GET(url, user_agent("Mozilla/5.0"))

  if (status_code(response) != 200) {
    cat("Erreur HTTP:", status_code(response), "\n")
    return()
  }

  html <- read_html(response)

  # Extraire et sauvegarder les données
  filename <- paste0(
    "niveau_",
    level,
    "_code_",
    params$code,
    "_codh_",
    params$codh,
    ".csv"
  )
  filepath <- file.path(OUTPUT_DIR, filename)
  if (!file.exists(filepath) || overwrite) {
    data <- extract_data(html)
    if (!is.null(data) && !is.null(nrow(data)) && nrow(data) > 0) {
      data.table::fwrite(data, filepath, row.names = FALSE, encoding = "UTF-8")
      cat("  -> Sauvegardé:", filename, "(", nrow(data), "lignes )\n")
    }
  }

  # Extraire les liens et continuer récursivement
  if (level < max_level) {
    links <- extract_links(html)
    if (length(links) > 0) {
      cat("  -> Trouvé", length(links), "liens\n")

      # Traiter les liens (avec option de limitation)
      links_to_process <- if (!is.null(max_links_per_level)) {
        links[1:min(max_links_per_level, length(links))]
      } else {
        links
      }

      for (link in links_to_process) {
        # Extraire les paramètres du lien
        if (grepl("codh=([0-9]+)", link)) {
          codh <- gsub(".*codh=([0-9]+).*", "\\1", link)
          next_params <- list(
            code = "",
            codh = codh,
            niveau = as.character(level + 1),
            annee = "2024",
            type_cat_etab = "pub",
            type_code = "cim"
          )
          scrape_recursive(
            next_params,
            level + 1,
            max_level,
            max_links_per_level,
            sleep_time
          )
        }
      }
    }
  }
}

# Fonction principale
scrape_atih <- function(
  max_level = 1,
  max_links_per_level = NULL,
  sleep_time = 0.5,
  overwrite = FALSE
) {
  cat("=== Scraper ATIH - Version rapide ===\n")
  cat("Niveau maximum:", max_level, "\n")
  cat("Dossier de sortie:", OUTPUT_DIR, "\n\n")

  # Paramètres de départ
  start_params <- list(
    code = "",
    codh = "000000000",
    niveau = "0",
    annee = "2024",
    type_cat_etab = "pub",
    type_code = "cim"
  )

  # Démarrer le scraping
  scrape_recursive(
    start_params,
    0,
    max_level,
    max_links_per_level,
    sleep_time,
    overwrite
  )

  cat("\n=== Scraping terminé! ===\n")

  # Afficher un résumé
  files <- list.files(OUTPUT_DIR, pattern = "\\.csv$")
  cat("Fichiers générés:", length(files), "\n")
  for (file in files) {
    cat("  -", file, "\n")
  }
}

# Pour tout récupérer (c'est long !). Certains fichiers ont l'air de manquer à la fin, on peut relancer avec overwrite = FALSE pour ne chercher que les manquants.
scrape_atih(4, sleep_time = 0.01, overwrite = FALSE)
