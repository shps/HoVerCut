package se.kth.scs.partitioning.algorithms;

/**
 *
 * @author Hooman
 */
public class Partition {

    private final int id;
    private int eSize;
    private int vSize;

    private int eSizeDelta = 0;
    private int vSizeDelta = 0;

    public Partition(int id) {
        this.id = id;
    }

    public void incrementESize() {
        this.eSizeDelta++;
    }

    public void incrementVSize() {
        this.vSizeDelta++;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @return the eSize
     */
    public int getESize() {
        return eSize + eSizeDelta;
    }

    /**
     * @param eSize the eSize to set
     */
    public void setESize(int eSize) {
        this.eSize = eSize;
    }

    /**
     * @return the vSize
     */
    public int getVSize() {
        return vSize + vSizeDelta;
    }

    /**
     * @param vSize the vSize to set
     */
    public void setVSize(int vSize) {
        this.vSize = vSize;
    }

    /**
     * @return the eSizeDelta
     */
    public int getESizeDelta() {
        return eSizeDelta;
    }

    /**
     * @return the vSizeDelta
     */
    public int getVSizeDelta() {
        return vSizeDelta;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Partition)) {
            return false;
        }
        Partition other = (Partition) obj;
        return other.getId() == this.getId();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + this.id;
        return hash;
    }

}
