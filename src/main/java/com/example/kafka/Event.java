package com.example.kafka;

public class Event {

    String filiaalNummer;
    String Datum;
    String Tijdstip;
    String klantNummer;
    String nasaNum;
    String productNaam;
    String inhoudAantal;
    String Eenheid;
    String Aantal;

    public Event(String filiaalNummer, String datum, String tijdstip, String klantNummer, String nasaNum,
                 String productNaam, String inhoudAantal, String eenheid, String aantal) {
        this.filiaalNummer = filiaalNummer;
        this.Datum = datum;
        this.Tijdstip = tijdstip;
        this.klantNummer = klantNummer;
        this.nasaNum = nasaNum;
        this.productNaam = productNaam;
        this.inhoudAantal = inhoudAantal;
        this.Eenheid = eenheid;
        this.Aantal = aantal;
    }

    public Event Stock(String filiaal, String product, Event event, String datum){
        if(event.filiaalNummer.equals(filiaal) && event.productNaam.equals(product) && event.Datum.equals(datum)){
            System.out.println(event);
        }
        return event;
    }

    @Override
    public String toString() {
        return "Event{" +
                "filiaalNummer=" + filiaalNummer +
                ", Datum='" + Datum + '\'' +
                ", Tijdstip='" + Tijdstip + '\'' +
                ", klantNummer=" + klantNummer +
                ", nasaNum=" + nasaNum +
                ", productNaam='" + productNaam + '\'' +
                ", inhoudAantal=" + inhoudAantal +
                ", Eenheid='" + Eenheid + '\'' +
                ", Aantal=" + Aantal +
                '}';
    }
}
