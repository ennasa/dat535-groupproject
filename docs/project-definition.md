# Prosjektdefinisjon for norsk fiskeridata

Status: foreløpig utkast, 25. oktober 2026. Datasettvalg, datavolum og detaljerte feltregler skal bekreftes gjennom profilering. Dette dokumentet beskriver planlagt arbeid, ikke gjennomførte resultater.

## 1. Prosjektbeskrivelse

Vi skal bygge en repeterbar PySpark-pipeline som omformer fangst- og landingsdata til kontrollerte datasett for analyse. Problemstillingen er: Hvordan varierer mengden og typen fisk som leveres på land, mellom norske regioner og gjennom året?

Resultatet skal støtte en analytiker som undersøker sesongmønstre og artsfordeling, gjennom månedlige oppsummeringer og et enkelt dashboard. Vi starter med ett komplett år og vurderer deretter 3–5 år. Sanntidsbehandling, maskinlæring og vurdering av fiskebestanden er utenfor første leveranse.

## 2. Datasett

Hovedkandidaten er Fiskeridirektoratets fangstdata koblet med fartøydata. Kilden publiserer komprimerte årsarkiver med semikolonseparert CSV i UTF-8 fra fangstår 2000. Inneværende og de to foregående årene oppdateres normalt hver natt unntatt natt til lørdag; eldre år ved behov. Verdiopplysninger frigis 12 måneder etter landing. Lisensen er NLOD.

Filstørrelse, radantall, sentrale felt og kvalitetsfunn er ennå ikke målt. Flere års radnivådata gir en mulig Spark-arbeidsmengde; faktisk behov og kjøretid skal vurderes på VM-en. Viktige risikoer er revisjoner, uklar radidentitet, ulike vektbegreper, manglende opplysninger og endrede geografiske eller artsrelaterte koder.

## 3. Bronze — hente og bevare

Vi laster ned valgte årsarkiver og bevarer originalfilene uendret. Et manifest registrerer kilde-URL, fangstår, hentetid, SHA-256, filstørrelse og run-ID. Filer organiseres etter kilde, fangstår og kildeversjon. Vi kontrollerer nedlasting og arkiv før behandling.

En planlagt jobb sjekker utvalgte kildefiler, eksempelvis ukentlig. Endret filhash utløser reprosessering av året; identiske filer lastes ikke inn som nye rader. Gamle kildeversjoner beholdes for sporbarhet. Ukjente skjemaendringer skal stoppe publisering og logges. Utviklingsmiljøet bruker et lite utvalg, mens produksjonsmiljøet bruker avtalte årsperioder.

## 4. Silver — modell, kvalitet og behandling

Silver er prosjektets hovedfokus og utformes sammen etter profilering.

- **Radnivå og nøkler:** Avklar om én rad er en seddellinje eller en annen detaljert registrering. Test kandidatnøkkel og undersøk eventuelle versjonsfelt. Vi skal ikke anta at én rad tilsvarer én levering, eller fjerne like rader uten faglig grunnlag.
- **Modell:** Bevar nødvendig detaljnivå og kildehenvisning. Skill landingssted, fangstområde og fartøyets hjemsted. Velg én dokumentert vektvariabel for hovedanalysen; ikke summer ulike vektbegreper. Definer håndtering av landinger utenfor Norge og av geografiske kodeendringer.
- **Rydding:** Bruk eksplisitt skjema, konsistente datoer, numeriske typer og dokumenterte kodeverk. Manglende verdiopplysninger skal ikke bli null kroner. Vurder avvik i mengder og datoer mot kildens regler før korrigering eller avvisning.
- **Kvalitet:** Mål lesefeil, manglende nøkkelfelt, nøkkelkonflikter, ugyldige typer og ukjente koder. Registrer aksepterte og avviste rader med årsak. Tall før og etter behandling skal kunne avstemmes; grenser for godkjenning fastsettes etter profilering.
- **Revisjoner:** Bygg berørte fangstår på nytt fra valgt kildesnapshot og erstatt kun tilsvarende data etter godkjente kontroller. Eventuelle revisjoner inne i filen håndteres etter dokumenterte kilderegler. Test at gjentatt kjøring gir samme resultat.
- **Spark og skalering:** Bruk DataFrame- og Spark SQL-operasjoner. Undersøk join, shuffle, partisjonering og skjev datamengde. Sammenlign en baseline med minst én begrunnet optimalisering på samme data og VM-oppsett. Registrer kjøretid og relevant informasjon fra kjøreplan/Spark UI. Øk årsutvalget bare når kapasiteten tillater det.

## 5. Gold — analyse og presentasjon

Hovedtabellen får én rad per landingsmåned, landingsregion og fiskeart, med samlet mengde i valgt vektenhet. Artsandel beregnes som artens mengde delt på total mengde for samme region og måned. Vektbegrep og håndtering av ukjent art/region dokumenteres. Fartøykategorier er en mulig utvidelse.

Dashboardet viser sesongmønstre og årssammenligninger, med filtre for år, region og art. Gold avstemmes mot relevante Silver-rader og oppdateres bare etter beståtte kontroller. Publiseringen viser datadekning, kildeversjon og siste vellykkede kjøring. Manglende perioder merkes tydelig og behandles ikke som null fangst.

## 6. Leveranse og drift

Kode, konfigurasjon og dokumentasjon versjoneres i Git. Endringer gjennomgås i PR og merges med squash. `PIPELINE_ENV` velger dev/prod-konfigurasjon. GitHub Actions kjører automatiske kontroller; planlagt pipelinekjøring skjer på VM via self-hosted runner når dette er satt opp.

Kontroller dekker filinnlesing, skjema, sentrale kvalitetsregler, gjentatt kjøring, rettelser og Gold-avstemming. Loggene viser status, radantall, avvisninger og kildeversjoner. README skal forklare oppsett, kjøring, reprosessering og gjenoppretting etter feil. Said leder data, del 1–3 og Gold; Sanne leder drift og del 6; Silver og PR-review gjøres sammen.

## Kilder

- [Fiskeridirektoratet — fangstdata koblet med fartøydata](https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata).
- Data Engineering Project Definition Guide, vedlagt i samtalen.
