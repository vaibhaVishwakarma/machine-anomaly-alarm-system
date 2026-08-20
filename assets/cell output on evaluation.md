Loaded 300 normal test samples and 354 anomaly samples.
Target Frame-Level FNR: <=26.30% (Min Detection >= 73.70%)
Processing Layer Rule : ALARM is set if >=3 of 5 predictions are ANOMALY
Device: cuda | Scoring: negative_logit

===================================================================================================================
  1. FRAME-LEVEL PERFORMANCE (ENFORCING FRAME FNR <= 26.30% PER MACHINE)
===================================================================================================================
Machine Threshold (tau) Target Logit Frame TPR (Detection) Frame FNR (Miss Rate) Frame FPR (False Alarm) Accuracy FNR <= 26.3%
  ID 00        -24.0857        24.09      74.83% (107/143)       25.17% (36/143)         16.00% (16/100)   78.60%       PASSED
  ID 02        -25.4341        25.43       73.87% (82/111)       26.13% (29/111)         21.00% (21/100)   76.30%       PASSED
  ID 04        -18.2705        18.27       90.00% (90/100)       10.00% (10/100)           0.00% (0/100)   95.00%       PASSED

===================================================================================================================
  2. PROCESSING LAYER CONSENSUS (>=3 OF 5 PREDICTIONS = ALARM)
===================================================================================================================
  BINOMIAL CONSENSUS FORMULA:
    * P(Alarm) = P(Anomalies >= 3 of 5) = 10*p^3*(1-p)^2 + 5*p^4*(1-p) + p^5
    * Factor 1: MISSED ALARM RATE = 1 - P(Alarm | p = Frame TPR)
    * Factor 2: FALSE ALARM RATE  = P(Alarm | p = Frame FPR)
-------------------------------------------------------------------------------------------------------------------
Machine Frame Detection Frame Miss (FNR) Frame False Alarm Layer True Alarm (>=3/5) MISSED ALARM RATE (1 - True Alarm) FALSE ALARM RATE (P(Alarm|Normal))
  ID 00          74.83%           25.17%            16.00%                  89.463%                            10.537%                             3.176%
  ID 02          73.87%           26.13%            21.00%                  88.425%                            11.575%                             6.589%
  ID 04          90.00%           10.00%             0.00%                  99.144%                             0.856%                             0.000%
===================================================================================================================