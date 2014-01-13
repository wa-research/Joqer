
namespace JoqerQueue
{
    public interface IReaderCursor
    {
        SequenceNumber Advance(SequenceNumber isn);
        SequenceNumber CurrentIsn();
        SequenceNumber MaxIsn();
    }
}
