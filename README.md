# get_atih_diagnosis
Extraction de la page des diagnostics du site scan santé

Data issues de https://www.scansante.fr/applications/statistiques-activite-MCO-par-diagnostique-et-actes

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
