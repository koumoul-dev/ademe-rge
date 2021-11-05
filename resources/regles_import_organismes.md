# règles pour l'import de fichiers en provenance des organismes

## Format des fichiers

- Le format des fichiers à transférer est le format CSV
- L’encodage du fichier est en ANSI (autre nom de la norme Windows-1252)
- Le fichier ne présente pas de ligne d’en-tête
- Chaque enregistrement (ligne) se termine par un retour chariot de type « Windows » (\r\n)
- Le séparateur de champ est le point-virgule « ; »
- Les champs ne doivent pas débuter et finir par « &quot; »
- Tous les champs sont matérialisés : si un champ est vide, il sera tout de même suivi par un
point-virgule (exemple : pour trois champs vides consécutifs en milieu de ligne, on aura
« ;;;; »)
- Les champs ne contiennent ni retour à la ligne, ni point-virgule, ni guillemets doubles « &quot; »

## Noms des fichiers

Les noms des fichiers sont les suivants (les autres noms sont) :
- entreprises.csv
- qualifications.csv
- liens.csv

## entreprises.csv

- Le champ « entreprise_id_organisme » est obligatoire. Il est limité à 25 caractères. Il est
généré à la discrétion de chaque organisme certificateur et doit être unique, ce champ ne
doit contenir ni espace, ni caractères spéciaux, ni caractères accentués.
Exemples : 1568 ou FAB1698 ou FABRIKAM123
- Le champ « siret » est obligatoire. Il doit contenir exactement 14 chiffres
Exemple : 12345678901234
- Le champ « nom de l’entreprise » est obligatoire. Il est limité à 100 caractères
Exemple : Fabrikam
- Le champ « Adresse ligne 1 » est obligatoire. Il est limité à 150 caractères
Exemple : 14, rue des pierres
- Le champ « Adresse ligne 2 » est facultatif. Il est limité à 150 caractères
Exemple : Bâtiment A
- Le champ « Adresse ligne 3 » est facultatif. Il est limité à 150 caractères
Exemple : BP 156
- Le champ « Code postal » est obligatoire. Il doit contenir exactement 5 chiffres, même pour
les départements de 01 à 09
Exemple : 69005 ou 09000
- Le champ « Ville » est obligatoire, il est limité à 55 caractères
Exemple : LYON CEDEX
- Le champ « Latitude » est facultatif. C’est un nombre décimal (séparateur décimal : la virgule
« , ») exprimé en degrés de -90 à 90 avec jusqu’à 6 décimales sans séparateur de
Exemple : -44,389673
- Le champ « Longitude » est facultatif. C’est un nombre décimal (séparateur décimal : la
virgule « , ») exprimé en degrés de -180 à 180 à partir du méridien de Greenwich avec
jusqu’à 6 décimales sans séparateur de milliers
Exemple : 3,378398
- Le champ « téléphone » est facultatif. Il doit faire exactement 14 caractères, sous forme de 5
paires de deux chiffres séparés par un espace. Seuls les numéros au format national sont
acceptés (pas de +33)
Exemple : 01 40 20 20 20
- Le champ « Email » est facultatif. Il est limité à 110 caractères
Exemple : personne@serveur.fr
- Le champ « site internet » est facultatif. Il est limité à 255 caractères, nous vous demandons
de bien faire attention à ce que l’adresse commence par http://
Exemple : http://www.fabrikam.com

## qualifications.csv

- Le champ « code » est obligatoire. Il est limité à 10 caractères alphanumériques. Ce champ
ne doit contenir ni espace, ni caractères spéciaux ou caractères accentués.
Exemple : 146 ou CHAUFCOND
- Le champ « nom de la qualification » est obligatoire. Il est limité à 200 caractères. Ce champ
sera affiché dans le site et devra être explicite pour le grand public.
Exemple : Pose de menuiseries extérieures et volets isolants

## liens.csv

- Le champ « entreprise_id_organisme » est obligatoire. Il est limité à 25 caractères et il est
généré par chaque organisme certificateur et doit être unique, il doit correspondre
obligatoirement à un champ entreprise_id_organisme  généré dans le fichier
« entreprises.csv ».
Ce champ ne doit contenir ni espaces, ni caractères spéciaux ou caractères accentués.
Exemples : 1568 ou FAB1698 ou FABRIKAM123
- Le champ « qualification_code » est obligatoire. Il est limité à 10 caractères
alphanumériques, il doit correspondre obligatoirement à un champ « code » généré dans le
fichier qualifications.csv
Exemple : 146 ou CHAUFCOND
- Le champ « date début » est obligatoire. Il est formé comme tel « aaaammjj » :
Exemple : 14 septembre 2011 devient 20110914
- Le champ « date fin » est obligatoire. Il est formé comme tel « aaaammjj »
Exemple : 14 septembre 2011 devient 20110914
- Le champ « url qualification » est obligatoire. Il est limité à 255 caractères, nous vous
demandons de bien faire attention à ce que l’adresse commence par http://
Exemple : http://www.fabrikam.com/urlcomplete?dossier=156
- Le champ « libellé certificat » est obligatoire. Il est limité à 255 caractères. Ce champ
s’affichera dans le site pour télécharge le certificat
- Le champ « particulier » est limité à un caractère : « 1 » pour les entreprises travaillant pour
les particuliers, « 0 » dans le cas contraire.

## Règles d’import

- Si un fichier est mal nommé, il est considéré comme manquant.
- Même si une seule erreur survient sur une seule ligne d’un seul fichier, aucune donnée
d’aucun des trois fichiers n’est enregistrée dans la base.
- Cependant, même en cas d’erreur, la lecture des fichiers continue afin de détecter un
maximum d’erreurs potentielles.
- Si tout est OK, les données sont intégrées en base.

## Journal des rejets

Si une ou plusieurs erreurs surviennent durant un import, un email (unique, quel que soit le nombre
d’erreurs) est envoyé au contact de l’organisme (il s’agit de l’adresse email du compte
d’administration de cet organisme sur le site). Le détail des erreurs est alors visible depuis une page
d’administration spécifique du site, accessible grâce aux identifiants d’administration du contact.