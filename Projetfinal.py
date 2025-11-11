#Phase 1 : Schémas & Sources 
bike_trips = [
    {
        "trip_id": "B001",
        "station_start": "Place Mohammed V",
        "station_end": "Gare Casa-Port",
        "duration_min": 14,
        "timestamp": "2025-11-04T08:10:00"
    },
    {
        "trip_id": "B002",
        "station_start": "Mosquée Hassan ll",
        "station_end": "Marché Central",
        "duration_min": 9,
        "timestamp": "2025-11-04T09:25:00"
    },
        {
        "trip_id": "B001",
        "station_start": "Place Mohammed V",
        "station_end": "Gare Casa-Port",
        "duration_min": 14,
        "timestamp": "2025-11-04T08:10:00"
    },
    
]
bus_routes = {
    "L9": {
        "name": "Ligne 9 - Hay Hassani ↔ Centre-Ville",
        "stops": ["Hay Hassani", "Oasis", "Place Mohammed V", "Marché Central"]
    },
    "L18": {
        "name": "Ligne 18 - Sidi Bernoussi ↔ Casa-Port",
        "stops": ["Sidi Bernoussi", "Aïn Sebaâ", "Derb Ghallef", "Gare Casa-Port"]
    }
}
metro_entries = [
    ("Place Mohammed V", 8, 7823),
    ("Derb Ghallef", 9, 310),
    ("Aïn Sebaâ", 8, 190),
    ("2 Mars", 8, -190)
]
known_stations = {
    "Place Mohammed V",
    "Gare Casa-Port",
    "Mosquée Hassan II",
    "Marché Central",
    "Hay Hassani",
    "Oasis",
    "Sidi Bernoussi",
    "Aïn Sebaâ",
    "Derb Ghallef"
}



# Phase 2 : Nettoyage & Déduplication

# Étape 1 : 
# Normalisation des noms de stations
def normalize_station_name(name):
    return name.strip().lower()

# Normalisation dans bike_trips
for trip in bike_trips:
    trip["station_start"] = normalize_station_name(trip["station_start"])
    trip["station_end"] = normalize_station_name(trip["station_end"])

# Normalisation dans bus_routes
for route in bus_routes.values():
    route["stops"] = [normalize_station_name(stop) for stop in route["stops"]]

# Normalisation dans metro_entries
metro_entries = [
    (normalize_station_name(station_id), hour, entries_count)
    for (station_id, hour, entries_count) in metro_entries
]

# Normalisation dans known_stations
known_stations = {normalize_station_name(station) for station in known_stations}

# Étape 2 : Déduplication de bike_trips par trip_id
clean_bike_trips = []
seen_trip_ids = set()

for trip in bike_trips:
    if trip["trip_id"] in seen_trip_ids:
        print(f" Doublon ignoré : {trip['trip_id']}")
    else:
        seen_trip_ids.add(trip["trip_id"])
        clean_bike_trips.append(trip)

bike_trips = clean_bike_trips

# Étape 3 : Suppression des arrêts de bus invalides
for route_id, route in bus_routes.items():
    original_stops = route["stops"]
    route["stops"] = [stop for stop in original_stops if stop in known_stations]
    removed = set(original_stops) - set(route["stops"])
    if removed:
        print(f" Arrêts supprimés de {route_id} : {removed}")

# Étape 4 : Filtrage des entrées métro invalides
metro_entries = [
    (station_id, hour, entries_count)
    for (station_id, hour, entries_count) in metro_entries
    if entries_count >= 0
]


# Phase 3 : Liens & Correspondances

# ÉTAPE 1 : Relier les trajets vélo aux stations via station_id
station_usage = {}  
# Ajout des trajets vélo
for trip in bike_trips:
    start = trip["station_start"]
    end = trip["station_end"]

    # Initialiser les stations si absentes
    for station in [start, end]:
        if station not in station_usage:
            station_usage[station] = {
                "bike_starts": 0,
                "bike_ends": 0,
                "metro_entries_total": 0,
                "bus_routes_served": []
            }

    # Incrémenter les compteurs vélo
    station_usage[start]["bike_starts"] += 1
    station_usage[end]["bike_ends"] += 1

# ÉTAPE 2 : Ajouter les entrées métro à station_usage
for station_id, hour, entries_count in metro_entries:
    if station_id not in station_usage:
        station_usage[station_id] = {
            "bike_starts": 0,
            "bike_ends": 0,
            "metro_entries_total": 0,
            "bus_routes_served": []
        }
    station_usage[station_id]["metro_entries_total"] += entries_count

# ÉTAPE 3 : Cartographier les arrêts de bus vers station_usage
for route_id, route in bus_routes.items():
    for stop in route["stops"]:
        if stop in station_usage:
            station_usage[stop]["bus_routes_served"].append(route_id)
        else:
            print("\n" + "-" * 180)
            print(f" Arrêt inconnu ignoré : {stop} (ligne {route_id})")

# ÉTAPE 4 : Valider les cohérences et détecter les anomalies
# Stations connues sans activité
inactive_stations = known_stations - set(station_usage.keys())
if inactive_stations:
    print("\n" + "=" * 180)
    print("Stations sans activité :")
    for s in inactive_stations:
        print(s)

# Anomalies de volume vélo/métro
for station, usage in station_usage.items():
    if usage["metro_entries_total"] > 5000 and usage["bike_starts"] + usage["bike_ends"] == 0:
        print("\n" + "=" * 180)
        print(f"\n Anomalie : forte affluence métro mais aucun trajet vélo à {station}")


# Phase 4 : Agregation et indicateur 
# Etape 1 Calcul des heures de pointe
from collections import defaultdict
from datetime import datetime
import os

# Initialisation
activity_by_hour = defaultdict(int)

# Métro
for station_id, hour, entries_count in metro_entries:
    activity_by_hour[hour] += entries_count

# Vélo
for trip in bike_trips:
    hour = datetime.fromisoformat(trip["timestamp"]).hour
    activity_by_hour[hour] += 1 

# Tri décroissant
peak_hours = dict(sorted(activity_by_hour.items(), key=lambda x: x[1], reverse=True))
print("\n" + "=" * 180)
print("Heures de pointe :")
for h, a in peak_hours.items():
    print(h, "h :", a)

# Etape 2 Calcul des scores de fréquentation
station_scores = {}

for station, usage in station_usage.items():
    # 5 min par trajet (approximatif)
    bike_duration_total = 5 * (usage["bike_starts"] + usage["bike_ends"])
    score = bike_duration_total + usage["metro_entries_total"]
    station_scores[station] = score

# Tri décroissant
busiest_stations = dict(sorted(station_scores.items(), key=lambda x: x[1], reverse=True))
print("\n" + "=" * 180)
print("Stations les plus fréquentées :")
for s, score in busiest_stations.items():
    print(s, ":", score)

# Etape 3 Lignes de bus les plus fréquentées
bus_line_scores = {}

for route_id, route in bus_routes.items():
    score = 0
    for stop in route["stops"]:
        score += station_scores.get(stop, 0)
    bus_line_scores[route_id] = score

# Tri décroissant
busiest_bus_lines = dict(sorted(bus_line_scores.items(), key=lambda x: x[1], reverse=True))
print("\n" + "=" * 180)
print("Lignes de bus les plus fréquentées :")
for l, score in busiest_bus_lines.items():
    print(l, ":", score)

# Etape 4 Calcul des ratios vélo/métro
bike_metro_ratios = {}

for station, usage in station_usage.items():
    total_bike = usage["bike_starts"] + usage["bike_ends"]
    total_metro = usage["metro_entries_total"]
    if total_metro > 0:
        ratio = round(total_bike / total_metro, 2)
    else:
        ratio = None  
    bike_metro_ratios[station] = ratio

print("\n" + "=" * 180)
print("Ratios vélo/métro :")
for s, r in bike_metro_ratios.items():
    print(s, ":", r)


# Phase 5 : Etape 1 Rapport financier multimodal
# Etaape 1 Calcul des revenus bruts

PRIX_MINUTE_VELO = 5.0  
PRIX_TICKET_METRO = 8.0  

revenu_velo = sum(trip["duration_min"] * PRIX_MINUTE_VELO for trip in bike_trips)
revenu_metro = sum(entries_count * PRIX_TICKET_METRO for _, _, entries_count in metro_entries)

# Etape 2 Calcul des revenus nets après taxe
TAUX_TAXE = 0.18  
revenu_velo_net = round(revenu_velo * (1 - TAUX_TAXE), 2)
revenu_metro_net = round(revenu_metro * (1 - TAUX_TAXE), 2)
revenu_total_net = round(revenu_velo_net + revenu_metro_net, 2)

# Etape 3 Calcul des revenus par station
station_revenue = {}

for trip in bike_trips:
    montant = trip["duration_min"] * PRIX_MINUTE_VELO
    for station in [trip["station_start"], trip["station_end"]]:
        station_revenue[station] = station_revenue.get(station, 0) + montant / 2 

for station_id, hour, entries_count in metro_entries:
    montant = entries_count * PRIX_TICKET_METRO
    station_revenue[station_id] = station_revenue.get(station_id, 0) + montant

top_10_stations = dict(sorted(station_revenue.items(), key=lambda x: x[1], reverse=True)[:10])

revenu_total = round(revenu_velo + revenu_metro, 2)
part_velo = round((revenu_velo / revenu_total) * 100, 2)
part_metro = round((revenu_metro / revenu_total) * 100, 2)

# Étape 4 – Construction du rapport financier
print("\n" + "=" * 180)
print("\t\t\t\tRapport_financier")

print("-" * 100)
print("Revenu total brut (DH)")
print(revenu_total)

print("-" * 100)
print("Revenu net après taxe (DH)")
print(revenu_total_net)

print("-" * 100)
print("Part vélo (%)")
print(part_velo)

print("-" * 100)
print("Part métro (%)")
print(part_metro)

print("-" * 100)
print("Top 10 stations rentables")
for station in top_10_stations:
    print(station)


print("\n" + "=" * 180)

# Phase 6 : Gestion des alertes
# Étape 1 : Détection des anomalies
SEUIL_ENTRIES_METRO = 5000        
SEUIL_MIN_VELO = 2                 
SEUIL_MAX_VELO = 180           


# Étape 2 : Génération des alertes et journalisation
# Initialisation des listes d'alerte et du journal système
alertes_saturation_metro = []         
alertes_trajets_velo_anormaux = []    
log_system = []                       

# Analyse des entrées métro
for station_id, hour, entries_count in metro_entries:
    if entries_count > SEUIL_ENTRIES_METRO:
        alertes_saturation_metro.append((station_id, hour, entries_count))
        log_system.append((
            f"2025-12-04T{hour:02d}:00:00",    
            "SATURATION",                      
            f"Station {station_id} > {entries_count} entrées"  
        ))

# Analyse des trajets vélo
for trip in bike_trips:
    d = trip["duration_min"]
    if d < SEUIL_MIN_VELO or d > SEUIL_MAX_VELO:
        alertes_trajets_velo_anormaux.append(trip["trip_id"])
        code = "VELO_COURT" if d < SEUIL_MIN_VELO else "VELO_LONG"
        log_system.append((
            trip["timestamp"],                  
            code,                               
            f"Trajet {trip['trip_id']} durée anormale : {d} min"  
        ))

# Affichage des alertes détectées
print("\n" + "=" * 180)
print("\n\t\t\t\t Alertes détectées : \n")
print("Alertes saturation métro :", alertes_saturation_metro)
print("Alertes trajets vélo anormaux :", alertes_trajets_velo_anormaux,)


# Étape 3 : Préparation des recommandations
# Identification des 5 stations les plus fréquentées
top_5_stations = list(dict(sorted(station_scores.items(), key=lambda x: x[1], reverse=True )[:5]).keys())

# Génération des recommandations pour les stations critiques
recommandations = []

for station, hour, entries_count in alertes_saturation_metro:
    if station in top_5_stations:
        recommandations.append(
            f" Renfort de personnel à la station {station} ({hour} heure )"
        )
print("=" * 180)
print("Recommandations :", recommandations)
print("=" * 180)

# Étape 4 : Journalisation des événements
print("Journal système :")
print(log_system)


# Phase 7 : Etape 1 
# Construction du rapport multimodal
rapport_stations = []

for station in sorted(station_usage.keys()):
    usage = station_usage[station]
    score = station_scores.get(station, 0)
    revenu = round(station_revenue.get(station, 0), 2)
    ratio = bike_metro_ratios.get(station)

    rapport_stations.append({
        "station": station,
        "bike_starts": usage["bike_starts"],
        "bike_ends": usage["bike_ends"],
        "metro_entries": usage["metro_entries_total"],
        "bus_routes": ";".join(usage["bus_routes_served"]),
        "score_frequentation": score,
        "revenu_total_DH": revenu,
        "ratio_velo_metro": ratio
    })

# Etape 2
# Export usage
csv_usage = "station\t\t\tbike_starts\tbike_ends\tmetro_entries\tbus_routes\n"
csv_usage += "\n".join([
    f"{r['station']:<16}\t{str(r['bike_starts']):<12}\t{str(r['bike_ends']):<10}\t{str(r['metro_entries']):<14}\t{str(r['bus_routes']):<10}"
    for r in rapport_stations
])

# Export revenus
csv_revenus = "station\t\t\trevenu_total_DH\tscore_frequentation\tratio_velo_metro\n"
csv_revenus += "\n".join([
    f"{r['station']:<16}\t{str(r['revenu_total_DH']):<18}\t{str(r['score_frequentation']):<20}\t{str(r['ratio_velo_metro']):<18}"
    for r in rapport_stations
])
# Export alertes
csv_alertes = "timestamp\t\t\t\tcode\t\t\tmessage\n"
csv_alertes += "\n".join([
    f"{ts}\t\t\t{code}\t\t{msg}" for ts, code, msg in log_system
])

# Afficher le contenu des CSV dans le terminal avant écriture
print("\n" + "=" * 180)
print("\n\t\t\t\t --- Contenu CSV: Usage ---\n")
print(csv_usage)
print("\n" + "-" * 180)
print("\n\t\t\t\t --- Contenu CSV: Revenus ---\n")
print(csv_revenus)
print("\n" + "-" * 180)
print("\n\t\t\t\t --- Contenu CSV: Alertes ---\n")
print(csv_alertes)
print("\n" + "=" * 180)

# Écrire les CSV sur le disque
script_dir = os.path.dirname(os.path.abspath(__file__))
usage_path = os.path.join(script_dir, "usage.csv")
revenus_path = os.path.join(script_dir, "revenus.csv")
alertes_path = os.path.join(script_dir, "alertes.csv")

try:
    with open(usage_path, "w", encoding="utf-8", newline="") as f:
        f.write(csv_usage)

    with open(revenus_path, "w", encoding="utf-8", newline="") as f:
        f.write(csv_revenus)

    with open(alertes_path, "w", encoding="utf-8", newline="") as f:
        f.write(csv_alertes)

    print(f" CSV écrits sur le disque ")
except Exception as e:
    print(f" Erreur écriture CSV : {e} ")
print("=" * 180)

# Etape 3
print("\n\t\t\t\t Activité horaire (heure → activité totale) ")

for i, (heure, activite) in enumerate(peak_hours.items()):
    print(f"{heure:02d}h : {activite} activités")

# Etape 4
# Phase Finale : Génération automatique de la conclusion

# Étape 4 : Conclusion
print("\n Conclusion :")

print(f"Le métro génère environ {part_metro}% des revenus, contre {part_velo}% pour les vélos.")
print("Les stations les plus rentables sont :", ", ".join(list(top_10_stations.keys())))

top_peak = list(peak_hours.keys())[:3]
print("Les heures de pointe identifiées sont :", " h , ".join([str(h) for h in top_peak]) + " h " )
if alertes_saturation_metro:
    print("Des alertes de saturation ont été détectées à :", ", ".join([s for s, _, _ in alertes_saturation_metro]))
else:
    print("Aucune alerte de saturation détectée.")
print("Il est recommandé de renforcer les ressources aux stations concernées pour améliorer la qualité de service.")



