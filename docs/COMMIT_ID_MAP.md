# Commit ID map (author rewrite of 2026-11-11)

Before publishing, all 71 commits were re-authored from the Git identity that was configured on the development PC
to the owner, `LImingcheng07 <2779249042@qq.com>`; the 6 version tags were recreated on the
rewritten commits with their original dates and messages. **Contents are unchanged**: every new commit has
the same tree (all files byte-identical), author date and message as the old one; this was checked for all 71.

Release notes, gates, tag messages, evidence files (`examples/*.json`, field `git_commit`) and commit
messages written before the rewrite cite the old IDs. Use this table to find the current commit. The original
history is kept by the owner as a verified Git bundle.

| # | Old ID | Current ID | Tag | Subject |
|---:|---|---|---|---|
| 1 | `86def0d` | `99e9a34` |  | chore: import audited FTDI Core v0.2 baseline |
| 2 | `6d2ae70` | `61fa35b` |  | docs: add project handoff materials (2026-11-06) |
| 3 | `d48f904` | `1dc521a` |  | feat: read-only archive-aware E2E audit (audit-e2e-archive, Task 001) |
| 4 | `a79a98c` | `16b0af7` |  | release: candidate manifest for Task 001 (unreleased development candidate) |
| 5 | `a62d8b3` | `6b86f6e` |  | test+fix: full-day real-data archive audit passes (gate A11); provenance SHA fix |
| 6 | `8cab86f` | `b04e3d6` |  | release: candidate manifest r2 for Task 001 (after gate A11) |
| 7 | `173892c` | `2519937` |  | build: pin LF for hash-pinned text files; guard against CRLF working copies |
| 8 | `5ce79d9` | `c6397b6` |  | release: candidate manifest r3 for Task 001 (reproducible from a clean checkout) |
| 9 | `eaccb77` | `c2de640` |  | build: check out .ps1 as LF to match shipped and baseline-pinned bytes |
| 10 | `c67fda2` | `749d936` |  | release: candidate manifest r4 for Task 001 (verified against a fresh clone) |
| 11 | `e2d17a7` | `78b93e1` |  | feat: Task 002 consumer retention design (ADR-007, proposed) + read-only planner |
| 12 | `7428560` | `30268a2` |  | perf: trace 6x Windows delivery slowdown to the HDD; durability-neutral checkpoint tuning |
| 13 | `8ef7d26` | `0fd1aea` |  | feat: Task 003 timestamp provenance (ADR-008) + encoding-safe CLI output |
| 14 | `2d65a31` | `55814de` |  | docs: consumer retention v2 interface contract (Task 002 phase 1) |
| 15 | `a3d018a` | `5315e22` |  | feat: consumer retention v2 (Task 002 phase 1): migrate, compact, rehydrate, tier-aware consumer and audit |
| 16 | `6509f3b` | `c16f327` |  | fix+test: four defects found by independent retention contract tests; lab-e2e audit selection |
| 17 | `9f52bba` | `fcf2ad9` |  | fix: planner reported a false concurrent modification on WAL consumers |
| 18 | `cb8c519` | `b9d3d40` |  | fix: six findings of the independent consumer-retention review (Codex) |
| 19 | `8b3d068` | `e920ae2` |  | test: full-day real-data consumer retention passes (gate T2-5, generated copies) |
| 20 | `8b09cf3` | `8cb8b24` |  | feat: v0.3.0 release preparation: tier-aware producer prune, version, release tooling, acceptance script |
| 21 | `afa457c` | `71bac65` |  | feat: Core archive-producer command (strict read-only dry run, chunked --apply, stop reasons) |
| 22 | `495e2b8` | `27efd19` |  | fix: eight findings of the independent archive-producer review (Codex) |
| 23 | `9d751fd` | `d8a8a90` |  | fix: follow-up review findings Q1-Q7 for archive-producer (Codex) |
| 24 | `f10533d` | `1ca68be` |  | fix: revalidate every deleted row against its archived record inside the prune transaction (Q8) |
| 25 | `ca9da87` | `428a26f` |  | docs: v0.3.0 release evidence (full-day acceptance PASS from f10533d) and final gates |
| 26 | `fad67d4` | `fa4092a` | v0.3.0 | release: FTDI-Core v0.3.0 package manifest (built from ca9da87) |
| 27 | `c301294` | `1c0d712` |  | docs: vacuum-db contract (v0.3.1 storage maintenance) |
| 28 | `610f285` | `9b5eb74` |  | feat: vacuum-db reclaims space by a verified rebuild that keeps the original (ADR-010, v0.3.1) |
| 29 | `e1aa541` | `d846ec2` |  | fix: vacuum-db redesigned after independent review (9 findings): exclusive lock, verified copy, in-place VACUUM |
| 30 | `d88bd92` | `12163fa` |  | fix: vacuum-db second review (7 findings): schema whitelist, structural sidecar checks, cleanup |
| 31 | `e44570e` | `66f8276` |  | docs: v0.3.1 release evidence (full-day acceptance PASS from d88bd92) and gates S1-S5 |
| 32 | `6fdf674` | `01abab6` | v0.3.1 | release: FTDI-Core v0.3.1 package manifest (built from e44570e) |
| 33 | `2efea5c` | `017da62` |  | docs+test infra: live capture contract (Task 004 phase 1) and loopback fake venue |
| 34 | `7021918` | `352479c` |  | feat: live read-only capture into sealed sessions (Task 004 phase 1) |
| 35 | `7bd8645` | `5890e28` |  | feat: compose a verified session chain into one replay source; latency evidence per session (Task 004 phase 2) |
| 36 | `d4dd5fd` | `40d985c` |  | fix: twelve findings of the independent live-capture review (Codex) |
| 37 | `6f1b471` | `0a92722` |  | fix: second live-capture review (M1-M7); v0.4.0 documentation |
| 38 | `5b6b7f9` | `f54f3d7` |  | fix: third live-capture review (N1-N6); v0.4.0 version, gates, loopback acceptance |
| 39 | `bff6f9b` | `f904b5b` |  | fix: fourth live-capture review; stable latency test; v0.4.0 counts |
| 40 | `a4b7a39` | `fa2e6cd` |  | docs: v0.4.0 release evidence (full-day and live loopback acceptance PASS from bff6f9b) |
| 41 | `16d7eae` | `68f9a24` | v0.4.0 | release: FTDI-Core v0.4.0 package manifest (built from a4b7a39) |
| 42 | `ea7258f` | `156cb68` |  | docs: real-network pilot of the v0.4.0 live capture (owner-approved): capture/audit COMPLETE, no latency |
| 43 | `340f580` | `01740a4` |  | fix(live): v0.4.1 clock evidence from the real-network pilot |
| 44 | `3327f9c` | `7209246` |  | fix(live): v0.4.1 review round 1 (Codex F1-F4) |
| 45 | `70c46b8` | `6bed5f6` |  | test(live): v0.4.1 mutation-check gaps closed (reversed perf, per-read drift margin, perf stamps reach the model) |
| 46 | `eac5a25` | `ff8aa3e` |  | test(live): post-connect deadline check observable (set-up ending after the budget) |
| 47 | `48182fb` | `46b5f35` |  | fix(live): v0.4.1 review round 2 (Codex R2-1..R2-7) |
| 48 | `50322d8` | `1333dac` |  | fix(live): reads-only heuristic also checks each read against its reference pair |
| 49 | `c8e9ead` | `6135798` |  | test(live): majority vote, not unanimity, in the reads-only heuristic |
| 50 | `d9b96d7` | `847b901` |  | fix(live): v0.4.1 review round 3 (independent review F1-F9) |
| 51 | `2537b31` | `63a4c7a` |  | docs: v0.4.1 pilot re-analysis figures (final rules) |
| 52 | `5989584` | `2187767` |  | test(live): p19 sends U+2028/U+0085 raw (escaped text never reached the evidence unescaped) |
| 53 | `884ebb1` | `041e67b` |  | fix(live): each read uses the clock sample giving it the smallest uncertainty |
| 54 | `269ed0a` | `70dfbe9` |  | test(live): same-wall-clock preference observable (p21) |
| 55 | `ff2145e` | `bdf39bf` |  | docs: v0.4.1 release evidence (full-day and live loopback acceptance PASS from 269ed0a) |
| 56 | `3fd4794` | `c6fcde8` | v0.4.1 | release: FTDI-Core v0.4.1 package manifest (built from ff2145e) |
| 57 | `4ca28de` | `7d482b1` |  | docs: second real-network pilot with v0.4.1 (owner-approved) |
| 58 | `64ed15f` | `299351b` |  | docs: third real-network capture (clock sample every 5 s): round-trip floor 43 ms, 20 ms limit unreachable from here |
| 59 | `f82601e` | `2d33b02` |  | fix(live): a REST body cut short is not an answer (first Linux run) |
| 60 | `e8e9def` | `714918f` |  | feat(live): live-capture --clock-interval-s (at least 1 s against the public venue) |
| 61 | `5c33cb4` | `7250af8` |  | fix(live): raw evidence reaches the OS before anything derived from it |
| 62 | `943cb8d` | `45040f0` |  | release prep: v0.4.2 (Linux verified, --clock-interval-s, remote-host results) |
| 63 | `6ef7d06` | `6163b5d` |  | docs: v0.4.2 release evidence (Windows full-day + live loopback, Linux live loopback, PASS from 943cb8d) |
| 64 | `fcbd248` | `af7f97d` | v0.4.2 | release: FTDI-Core v0.4.2 package manifest (built from 6ef7d06) |
| 65 | `bc0c4fc` | `2bac8cf` |  | docs: one-hour evening capture on the Linux server (54,438 events, audit COMPLETE, P50 35.4 ms: per-connection network path) |
| 66 | `5ab05ab` | `c92f064` |  | feat(live): path selection (make-before-break probe connections; keep the faster delivery path) |
| 67 | `d3474cd` | `1f8074f` |  | test: no test can reach a real host (loopback-only guard in fakevenue) |
| 68 | `20d6e33` | `0df67f9` |  | fix(live): probe connections are set up in the background (never hold up the writer) |
| 69 | `fd28e09` | `472f07a` |  | release prep: v0.4.3 docs (path selection results on the real network) |
| 70 | `342bb75` | `e7cf491` |  | docs: v0.4.3 release evidence (live loopback PASS on Windows and Linux from fd28e09; full-day not re-run: ZIP unavailable) |
| 71 | `a32f01b` | `3181e11` | v0.4.3 | release: FTDI-Core v0.4.3 package manifest (built from 342bb75) |

Full IDs (old, current):

```
86def0de3f8060afc3c28002f10729ccf8d1d1b7 99e9a3445610bbc53b101d0f2242d9c8539961a6
6d2ae70b8be731437eebadcd6391997b21fc4001 61fa35bdd046286ca16b1825bfd6b88ea0ed4ea1
d48f904a223bcd42968023b69713495818d6fd64 1dc521a4f72785ba5f02e244be8218e4016ee978
a79a98c8c0ea4a19b1108b3c6f15ace8af249d82 16b0af7de8ba52fe237157ca2c3493717ce1738f
a62d8b3818f63925443676bd8e38b19218bc443d 6b86f6e2b82ef80c59024a1138452ebdd1569229
8cab86faa7ee36d12a935ee00d2aca0edd0c369d b04e3d67a73502d82061b6fae461b8e9a55ff63b
173892cd259cc45fa5146b38ff5e1668c5b5531c 2519937739fe8810f608e050e2232f53429a2184
5ce79d99472b1001fc4051dcba2767c831bd7ca2 c6397b651e1987057b107ef674a1ee151914d6d5
eaccb7787bae9f7d01399e067788804fc72e0a13 c2de64091405f0ee43a3cdde118157456ffbd61c
c67fda26d6756b5fa39fcdf777956f9eac037472 749d9362366c029375ffe1deb9a34c8c5577ca87
e2d17a706c968ce1b43dc86621bcd29400978ac6 78b93e118ec1a71038c0f122e3133198165878e6
74285603d938de9a976c73220bf768386ba80d68 30268a2f9297e80fc71e34b0e6acd25695657669
8ef7d26c20fbeaf5efc2500f2779ab3ca480accc 0fd1aea0a502a3664383f4ce98751190f5ae7219
2d65a31cfb6f4a4f05a57d9a26d093b5aec37e50 55814dec1e0966235d2173521acd69a511468544
a3d018a739073cd566aa04f79cea33d1029c5ee8 5315e22b420c691c2b8c426cb2e9c6dfcfa011f0
6509f3b958e009ea83afd3185abe20c206a0a215 c16f327f46d2e87ede44e1dcbfd8f014e7c1f7a6
9f52bba327167590b2f929912521864a331e6adf fcf2ad91875b6215742b5da541ed818402171da2
cb8c519eee43dad7fe64f1a045b1ebad76be69ea b9d3d4071f8e0a502bb330041e115706eecd26eb
8b3d06836e8c43877441a78536138af59389a1d0 e920ae22b9fd78c146536af9f713b31cf3df45b9
8b09cf3dc748df8e74806c359cccaeb785c780db 8cb8b245f06de67ab303b3983db5e277d0dec449
afa457c340f2017c3b925551875e4baa03d61e8a 71bac65c70e3b170d5a5c23fcde71d36183575f5
495e2b853087c9d96d8895a7b0f732cca8858be5 27efd19d44afa7fb848d62a6e8aa735fdd37d2df
9d751fd55d63929b3c322c8ac24025243e2c9a13 d8a8a90846cf6fe165146dee701011f6ec8cd38d
f10533dc8f874959a99162a8c00ce9035ff25ea5 1ca68be73cf834df3b645348e1c160874e338a7c
ca9da87166392305217d3f01d1bad9b8ca5f6a30 428a26f0f1e13c13ca81e9033af67b544de317a3
fad67d4b2528dae29e842bc7bcf4353451ab665a fa4092a2b5e40eaa18643c8e080fb46bc03ca951
c301294b8830b0f3d29fa13cf36a9345e45f16db 1c0d71220aa375b78cbedb47805a759280d92994
610f285aee0b6dd3fcd22eeb95407019f6e30ca7 9b5eb74933a5c4dd11f7637aa0650a7492b39c25
e1aa5419267dd5071795b879e64060350bccb281 d846ec2164fb81d6d0011b5c61e560e638378d59
d88bd920ae45c83d587ee6f3dc3931c78b841e1e 12163fa8bb2623ac8294436b486c2e1e7fb40326
e44570e46cb8526f4d2f57998146445a06aba44d 66f8276340651c9381d90dd827a9fa7a4ef5cf18
6fdf674abe36ab23780654589a9b5114aa441382 01abab6931bf257d9a1b4ece18c556d4309af73f
2efea5cb5ce5bf04cbe93c030ed6075bf8a32248 017da62529205688dc256c126b7be3fdeefd845a
702191886279544fb8f99e2ddfc9aac113f0f986 352479c4e7c5ec3bd778c6ae257966c5bd163e2f
7bd864563642c115818bbed2b1387813e90a7252 5890e282754d297da5746c878c8776fb2e2b9582
d4dd5fd53ac38e6d0aa42a2fdf0e4b34a746db80 40d985c77f4070f7d387d7dd52bc96ff3579d740
6f1b47145c505cf238e821c0f9eb114ae418f1d3 0a92722332858fb6cf5c000f4ece0e3293c0a129
5b6b7f907d5c44cb4b922ad1b2787a796cb4c38d f54f3d70aa9117c5aae572ae304c8a33ce906ba2
bff6f9b27775c1aa4c138bb8838cddb28f93eb97 f904b5b60836a2547d6c127458df5010de9e65f6
a4b7a3975e2a47355bc3ee4025223c26c86ecf80 fa2e6cdcc2e86561e93762c6512fbcbd8102913d
16d7eae68a18e33e2a3fbaaf009560185f2b1c03 68f9a24cdfba0d743ab49070c2344577712dead1
ea7258fc1fa06bd6530346530dfdf148e930eda3 156cb68fb64046dfe7f5161342ba47c50f095e78
340f5808f00be1d07c8c3b900fc7d68c5074d2ec 01740a45d758ef686e68ca2fd00905fe1438571a
3327f9cf8dfac50856dd5be746be053a26622d96 7209246441c63c99abb42d7466c261ed8627b3e5
70c46b8ec97ef247f65a2bc1fef55372ea46db2c 6bed5f6aa14dd2e845055ed0f422ed15c4539762
eac5a252c06398ab382ac4ddf11c539de82b4359 ff8aa3effce5e0647d437ade1ccd3ad4602a9257
48182fbf08d019e5d67558942526734da728f997 46b5f35ab4548cf160fa5f2051751901071fc927
50322d867a8b6d1f18ce41662a7adaa86cf8f7c0 1333dacbbe5940e5a2a7e00fda19b4d4b41eb99f
c8e9eadb6f39d376c8f80579a746a93fd1f2a6bd 61357987e4f092a798b824ab6969d7ee40ace0fd
d9b96d77be87dc3a5e56c0136148e1c3053efda3 847b9014d26873a4cc2f7f9a9a696b6b0aeb627c
2537b31d8f9584eb1be4cbebb1094f9c7beccfdd 63a4c7a71de424d2413b02894c9d242c4746b64c
59895844b022f609330faec30302da2e5c750e7d 218776794e02440aa9b1fcd78a9685e7d6108e74
884ebb1f720570dcc31544add3cb8b4b7a6f0f82 041e67be6c3dc0c931179a4138e2e863b91fc497
269ed0a863da52c0783574f3e8a72788e984a9d3 70dfbe95d1d729d5499f516ce8a64b7d0c990af1
ff2145eb0300d87171989abf7019675760fae1a2 bdf39bf92b685e3541fcc0ed4500896f2e057ac9
3fd47943fdad6c97eac90396aaa353716664dc49 c6fcde840ed5e81244e504d2a2f23b778f7d7958
4ca28def9f913cc441d8dd98fd9e8cebc4c8310d 7d482b1436d7b932a556ddc2092e96beb6a914ea
64ed15f373ff9bcd0f8cdf777e63a1af2456dd3c 299351b807bf4e9640bd1d97dc1ee1dbb776596a
f82601e658ad00cdf0288026286910b02c5bc297 2d33b02eec7e67e038acc9df76adeb199283b8f6
e8e9defea9ab70b991b5eb2080d203db4d63c848 714918f26d4b16a65006764c381faedf66f07227
5c33cb41c6fda16b9983e10cf424119ae8d156c3 7250af8db129d3fe4c6995b82e865b00c67c83c6
943cb8d8b8afb57c75336503768e683f21e07496 45040f0b64be8e3041235d62da6194cfa9f48453
6ef7d06188554c6156191692393f7ffc6b12933d 6163b5dff9bf7dc9dfcc4472b87df68ccc5d3b4c
fcbd2480dea6bce321da24bb5d36338ceeba23fa af7f97dc2d683e98e1e2ce4c8ad0fb328963cd4f
bc0c4fc6e4053fdca7e562143199bd8b496d9821 2bac8cf05aab32b2cb26319937c09a2191cfeb77
5ab05ab560bbe85aeeb387e9364d29ebe4565617 c92f0641e56edec4cf88c28adbf47c7ffdec081f
d3474cda946d00c61101286c3669804a88c3eb56 1f8074f2fac6d1a202c7b82482cdd52853b7cbd7
20d6e3350bf2156599314aa580f4f4cd76c53220 0df67f93e43486bfe494209941cb67c832730257
fd28e09c9ecda81dd1d7ac1821b212a7aec8422f 472f07a3c1bb8ab48e8f38a2eb916f1d3a5adc1a
342bb75ce6d5ca2940a1e93eb068a23892bce312 e7cf491876838f6d78e846e548c138d7b3d330a3
a32f01bbf4e1fac424a3081a49b71d7915303eed 3181e11cf3ab47638ac8cb699ef771b8eefbf312
```
