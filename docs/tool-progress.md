# Progression native Hermes dans Zulip

Le plugin édite désormais le contenu de la bulle d’activité native. L’API reçoit
uniquement `message_id` et `content`; aucun déplacement ni changement de topic.
Le serveur reste l’autorité sur les droits d’édition du bot. Aucun réglage serveur
ni droit administrateur supplémentaire n’est appliqué par le plugin.

Le moteur installé distingue deux capacités : il détecte la méthode
`edit_message` pour la progression des outils, mais utilise
`SUPPORTS_MESSAGE_EDITING` pour le streaming des réponses. Ce dernier reste
`False` : les réponses complètes suivent `send`, qui retire les blocs de contrôle
et exécute une seule fois leurs actions de hiérarchie selon la politique existante.
Les éditions d’activité n’exécutent jamais ces actions. Aucun faux transport draft
n’est introduit. Pour la voie proxy (qui peut ignorer ce drapeau), conserver le
streaming de réponses désactivé dans la configuration; ce chemin n’est pas qualifié.

Le regroupement et la fréquence (1,5 seconde) restent ceux de Hermes. La limite
annoncée au moteur respecte le budget de découpage du plugin, plafonné à 10 000
caractères, après réservation du préfixe. Le moteur ouvre une continuation lorsque
nécessaire. Une édition hors budget est refusée sans tronquer silencieusement.

Au premier refus, timeout ou erreur réseau d’une édition, cette bulle est gelée.
Son identifiant est mémorisé dans un cache limité à 256 échecs : les mises à jour
suivantes n’appellent plus Zulip. Le résultat est marqué `retryable=True` pour
empêcher le comportement de repli du moteur, qui créerait un message par outil.
C’est un signal de compatibilité avec le consommateur, pas une promesse de nouvelle
tentative réseau. La réponse finale est envoyée normalement; le bloc d’activité
peut rester incomplet. Le cache disparaît au redémarrage, ou évince les entrées
les plus anciennes après 256 autres échecs. Une requête synchrone expirée peut
encore se terminer dans son thread; elle n’est pas rejouée par l’adaptateur.

## Proposition Basile (non appliquée)

Fusionner dans son seul profil, en préservant les autres valeurs :

```yaml
display:
  tool_progress_command: true
  cleanup_progress: false
  platforms:
    zulip:
      tool_progress: all
      tool_progress_grouping: accumulate
```

`/verbose` sans argument parcourt `off → new → all → verbose → log → off`.
Le choix est enregistré pour la plateforme Zulip du profil, donc concerne ses
conversations Zulip et pas seulement le MP courant. Il ne règle pas la longueur
des réponses. Ne pas annoncer `/verbose all` : le gestionnaire ne traite pas cet
argument. Le chargement explicite d’une skill via `skill_view` apparaît comme un
appel d’outil; cela ne certifie ni usage implicite ni réussite de la tâche.
Conserver les réglages actuels de Comptable et KMS.

## Validation et retour arrière

Les tests du plugin couvrent les éditions DM/canal, identifiants, budget, préfixe,
contrôles, erreurs et annulation. Le test externe `validation/test_native_progress.py`
utilise directement `TurnRunner.send_progress_messages` du moteur installé avec
le vrai adaptateur et un transport simulé, sous profil temporaire sans réseau.
Il vérifie le regroupement, le gel après refus, la destination et la réponse finale.
Le moteur peut propager l’annulation pendant son sommeil en file vide sans vidage
final; les dernières lignes d’activité ne sont donc pas garanties à l’interruption.
La recette réelle Zulip et les permissions serveur restent à confirmer avec une
destination explicitement autorisée; aucun envoi externe n’a été effectué.

Retour arrière : restaurer la révision précédente du plugin et le fichier de
configuration sauvegardé du seul profil modifié, puis recharger après drainage.
Ne pas restaurer ni supprimer les historiques.
