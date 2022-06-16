package com.example.kafka;

public class Transaction {

    String primaryKey;

    String klant;
    String productID;
    String productNaam;
    String aantal;
    String filiaalNummer;
    String Datum;
    String Tijd;
    String price;


    public Transaction(String primaryKey, String klant, String productID, String productNaam, String aantal,
                       String filiaalNummer, String Datum, String Tijd, String price) {
        this.primaryKey = primaryKey;
        this.klant = klant;
        this.productID = productID;
        this.productNaam = productNaam;
        this.aantal = aantal;
        this.filiaalNummer = filiaalNummer;
        this.Datum = Datum;
        this.Tijd = Tijd;
        this.price = price;

    }
    public Transaction GenerateTransactions(Transaction transaction, String datum, String tijd, String filiaalNummer,String klant ){
        if(transaction.filiaalNummer.equals(filiaalNummer) && transaction.Datum.equals(datum) &&
           transaction.Tijd.equals(tijd)&&transaction.klant.equals(klant))
        {
            System.out.println(transaction);
        }
        return transaction;

    };

    @Override
    public String toString() {
        return "Transactie{" +
                "primaryKey='" + primaryKey + '\'' +
                ", klant ='" + klant + '\'' +
                ", productID='" + productID + '\'' +
                ", productNaam='" + productNaam + '\'' +
                ", aantal='" + aantal + '\'' +
                ", filiaalNummer='" + filiaalNummer + '\'' +
                ", Datum='" + Datum + '\'' +
                ", Tijd='" + Tijd + '\'' +
                ", price='" + price + '\'' +
                '}';
    }
}
