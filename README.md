# get_atih_diagnosis
Extraction de la page des diagnostics du site scan santé

Data issues de https://www.scansante.fr/applications/statistiques-activite-MCO-par-diagnostique-et-actes

**UPDATE**

Utilisation d'une version optimisée du scraper avec parallélisation et cache des liens.

Voir le fichier `parallel_scraper.R`.

```
scrape_atih_optimized(
  max_level = 4,
  sleep_time = 0.01,
  overwrite = FALSE,
  use_cache = TRUE,
  n_workers = 8  # Ajuster selon votre machine
)
```

Il faut le lancer plusieurs fois pour arriver au bout ... Surveiller dans git les nouveaux fichiers qui apparaissent.

Après cela regarder le notebook `post_scraping.qmd` pour l'aggrégation des différents CSV créés.


**Ancienne version**

Exemple d'utilisation du script

```
scrape_atih(max_level = 2)
```

Pour aller creuser dans l'arborescence jusqu'au deuxième niveau.

D'autres paramètres existent:

```
max_links_per_level : nombre de liens à récupérer par niveau (NULL par défaut pour tout récupérer

sleep_time : période de repos entre 2 requêtes (0.5 secondes par défaut)
```

