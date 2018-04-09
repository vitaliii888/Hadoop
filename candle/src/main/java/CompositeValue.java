import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements Writable{
    private String TIME;

    private Long ID_DEAL;
    private Double PRICE_DEAL;

    public String getTIME() {
        return TIME;
    }
    public Double getPRICE_DEAL() {
        return PRICE_DEAL;
    }
    public Long getID_DEAL() {
        return ID_DEAL;
    }


    public void setTIME(String TIME) {
        this.TIME = TIME;
    }
    public void setID_DEAL(Long ID_DEAL) {
        this.ID_DEAL = ID_DEAL;
    }
    public void setPRICE_DEAL(Double PRICE_DEAL) {
        this.PRICE_DEAL = PRICE_DEAL;
    }

    public void set(String TIME,Long ID_DEAL,Double PRICE_DEAL){
        this.TIME=TIME;
        this.ID_DEAL = ID_DEAL;
        this.PRICE_DEAL = PRICE_DEAL;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(TIME);
        dataOutput.writeLong(ID_DEAL);
        dataOutput.writeDouble(PRICE_DEAL);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        TIME=dataInput.readUTF();
        ID_DEAL=dataInput.readLong();
        PRICE_DEAL=dataInput.readDouble();
    }
    @Override
    public String toString(){
        return String.format("Time = %s\t ID=%d\t Price=%3.2f",TIME,ID_DEAL,PRICE_DEAL);
    }
}
