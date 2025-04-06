# Evaluare implementare generator Pub/Sub

## 1. Tipul de paralelizare

Paralelizarea este realizată folosind **threads**, prin intermediul clasei `ExecutorService` cu un `FixedThreadPool`.

---

## 2. Factorul de paralelism

Programul a fost testat cu următorii factori de paralelism:

- **1 thread** (fără paralelizare)
- **4 threads**
- **8 threads**

---

## 3. Numărul de mesaje generat

- **Publicații generate:** 100.000
- **Subscrieri generate:** 100.000

---

## 4. Timpii obținuți

| Număr de Threads | Timp execuție totală |
|------------------|----------------------|
| 1                | 735 ms              |
| 4                | 600 ms              |
| 8                | 500 ms              |

---

## 5. Specificațiile procesorului

Testele au fost rulate pe următorul sistem:

- **Procesor:** AMD Ryzen 7 7435HS 3.1Ghz, 8 cores / 16 threads
- **Sistem de operare:** Windows 11

---

## 6. Observații

- Fiecare thread generează o porțiune din publicații și subscrieri în paralel.
- Datele sunt scrise în fișierele `publications.txt` și `subscriptions.txt`.
- Se verifică la final dacă frecvențele câmpurilor și proporția operatorilor de egalitate corespund configurației specificate.
