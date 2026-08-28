from pyspark.sql import SparkSession
import sys

if __name__ == '__main__':
    # Le chemin par défaut est /tmp/parquet_output si non spécifié
    path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/parquet_output'
    
    # On crée une session locale juste pour lire le fichier
    spark = SparkSession.builder \
        .appName('ReadParquetTest') \
        .master('local[*]') \
        .getOrCreate()
        
    try:
        print(f"\n[{'*'*10} LECTURE DU DOSSIER PARQUET {'*'*10}]")
        print(f"Chemin : {path}")
        
        df = spark.read.parquet(path)
        
        print("\n--- Schéma des données ---")
        df.printSchema()
        
        print("\n--- Aperçu des données (20 premières lignes) ---")
        df.show(20, truncate=False)
        
        print(f"--- Nombre total d'enregistrements : {df.count()} ---\n")
        
    except Exception as e:
        print(f"\n[ERREUR] Impossible de lire le fichier Parquet : {e}")
        print("Avez-vous bien lancé le pipeline et attendu quelques secondes ?\n")
