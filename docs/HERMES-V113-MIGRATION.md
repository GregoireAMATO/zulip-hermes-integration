# Migration du fork vers 1.13.0 — 19 octobre 2026

Base : tag `v1.13.0` (`b47930d`). Ancien état : `ba10c62`, 12 commits après 1.8.1.
Les chemins ci-dessous désignent le code et les tests du tag amont, sauf mention « report ».

| Commit ancien | Décision et preuve |
| --- | --- |
| `d8e82e4` | Portée couverte par `runtime_scope.get_setting`, `snapshot_settings` et `tests/test_profile_isolation.py` (#156). Reporter uniquement `api_token`, absent de l'amont et requis par la garde multiplex du moteur ; assertion ajoutée au test de deux profils. |
| `75e64cf` | Abandonné : `outbound.send_typing` / `stop_typing` et `tests/test_typing_hooks.py` couvrent la boucle de saisie du moteur pendant tout le tour (#129). |
| `7cd180e` | Hiérarchie/config/directives abandonnées : aucune clé `thread_hierarchy`, directive `zulip_thread_action` ni commande `@thread` dans les configurations/consignes actives et les deux dépôts inspectés. Les hooks de cycle de vie sont repris en amont (`adapter.on_processing_start/complete`, `tests/test_trace_lifecycle.py`, `tests/test_silent_run_notice.py`). Les sessions par topic et `[[zulip_topic: …]]` restent assurées par `routing.py`, `outbound.py` et leurs tests. Conserver la désactivation du streaming avec le report de progression. |
| `70b8045` | Reporté dans `inbound.py` : `source.message_id` manquait encore sur les chemins stream/DM. Deux assertions de livraison dans `tests/test_integration.py`. |
| `53e3977` | Reporté : champ `message_id` du stub, indispensable au contrat précédent ; vérifié contre `BasePlatformAdapter.build_source` installé. |
| `c7b6f62` | Le routage natif est couvert par `commands.py`, `tests/test_commands.py` et `tests/test_slash_command_gate.py` (#190, #259). Reporter la politique DM avant toute commande, encore placée après les commandes du plugin en amont ; tests DM refusés + commandes natives transmises dans `tests/test_integration.py`. |
| `3d99c4c` | Reporté en déléguant vers `outbound.edit_message` : contenu seul, budget incluant le préfixe, gel après échec, cache limité à 256, réponse finale indépendante. `tests/test_progress_editing.py` repris et complété pour les secrets, le raisonnement et la borne du cache. |
| `e7b12b9` | Abandonné : résolution des URI contre l'origine dans `media.upload_file_to_zulip`, `tests/test_media_outbound.py`, `tests/test_media_inbound.py` (#121, #126). |
| `39f62d2` | Test ancien remplacé par les tests amont de livraison native d'images dans `tests/test_media_outbound.py`. |
| `6cf23db` | Abandonné : `outbound.send_image_file` et `_send_uploaded_media`, `tests/test_media_outbound.py` (#121). |
| `bb225ce` | Test ancien remplacé par le contrat amont `send_document` dans `tests/test_media_outbound.py`. |
| `ba10c62` | Abandonné : `outbound.send_document` partage le transport natif testé en amont (#121). |

## Configuration et comportement

- 1.9 : pièces jointes natives, HTTPS requis et contrôle des répertoires d'envoi ; `HERMES_MEDIA_ALLOW_DIRS` reste le mécanisme pour des fichiers hors des racines autorisées. Saisie confiée au moteur, pas à une seconde boucle du plugin.
- 1.10 : portée des profils et de leurs données, garde contre les secrets, suppression des blocs de raisonnement, commandes natives transmises au moteur ; trace, file par session, historique et réactions déclenchantes sont optionnels. Les réactions du kit restent distinctes des réactions déclenchantes du plugin.
- 1.11 : `ZULIP_PROFILE=recommended` activerait mention obligatoire, allowlist DM du propriétaire, trace, historique, observation, file par session et réactions déclenchantes. Ne pas ajouter ce marqueur aux profils existants : leurs réglages explicites et les valeurs par défaut sont conservés.
- `ZULIP_TOPIC_SESSIONS=true` conserve désormais la session au renommage via un identifiant de conversation persistant. `routing.migrate_legacy_topic_sessions` reprend les anciennes clés au branchement du SessionStore ; `/topic-sessions` liste et `/continue` sélectionne les sessions. Aucun effacement des historiques.
- 1.12 : tours vides/échoués signalés ; trafic des bots sans prolongation de l'engagement ; `/approve` et autres commandes ne sont plus filtrés par les mentions. Restrictions d'approbation du plugin optionnelles ; ne pas activer son preset au-dessus des validations métier du kit.
- 1.13 : reprise des messages de flux adressés, déduplication commune, détection de poll muet et réenregistrement, backpressure bornée, pièces jointes entrantes transmises au moteur. La reprise peut exécuter un travail ancien non traité ; surveiller les tours au démarrage.
- `ZULIP_STREAMS`, `ZULIP_CHATMODE`, `ZULIP_DM_POLICY`, `ZULIP_ALLOWED_USERS`, `ZULIP_REACTIONS_ENABLED` restent explicites. Les profils du kit conservent leurs réactions désactivées et `onmessage` ; pas de modification de `.env` nécessaire.
- `display.platforms.zulip.tool_progress`, `streaming`, `interim_assistant_messages` et `zulip.gateway_restart_notification` sont des réglages du moteur. Garder les valeurs des profils ; le transport d'édition ajouté ne les active pas. La progression est testée dans un profil E2E temporairement configuré à cet effet.

## Vérification et déploiement

Contrôler `scripts/check_compat.py` contre le Python réellement installé, sous `MemoryMax=700M`, `MemorySwapMax=100M`, puis compiler/importer sans ouvrir de connexion. La version détectée est 0.21.0 (plancher du plugin 0.18.2) ; 4 commandes du plugin vérifiées contre les 83 du moteur. Les boutons exec sont absents et protégés ; le routage `thread_id` passe. Le hook précis d'arrêt de saisie manque sur ce moteur : repli amont sur le dernier topic vu, sans incidence sur les réponses.

Avant checkout : branche `avant-1.13`, archive hors dépôt des fichiers non suivis/ignorés, et copie privée des index de sessions. Ne jamais exporter les valeurs des `.env`. Redémarrer chaque unité seulement après `active_agents=0` avec état frais et contrôle du journal, miroir d'abord ; vérifier connexion, erreurs et mémoire avant la suivante. AI Desk sans Zulip est exclu.

La simulation locale `tests/test_resilience.py::TestPollLiveness::test_a_synthetic_stall_is_detected_and_forces_re_registration` prouve la détection et l'expiration de queue sans bloquer la production. Les E2E doivent rester dans `bots-e2e`, avec le compte testeur et Bot-kit Demo ; KMS : connexion et abonnements uniquement.

## Retour arrière

En cas d'échec : arrêter la séquence, attendre la fin des tours des unités déjà basculées, `cd ~/.hermes/plugins/zulip && git checkout avant-1.13`, puis redémarrer ces mêmes unités une par une et vérifier leur connexion. Ne pas redémarrer AI Desk. Conserver les archives et les nouveaux registres hors git ; ne pas restaurer aveuglément les index sauvegardés après de nouveaux tours (cela masquerait leurs sessions). Les anciennes clés de session sont renommées par l'amont : en cas de retour durable, rapprocher les index avec la sauvegarde avant toute restauration.
