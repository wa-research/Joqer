
namespace JoqerQueue
{
    public struct PageCount
    {
        public const int PageSize = 4096;

        public PageCount(int pages)
        {
            Pages = pages;
        }

        public int Pages;
        public long Bytes { get { return Pages * PageSize; } set { Pages = (int)(value / PageSize); } }

        public static PageCount operator +(PageCount p1, PageCount p2)
        {
            return new PageCount(p1.Pages + p2.Pages);
        }

        public static PageCount operator +(PageCount p, int pages)
        {
            return new PageCount(p.Pages + pages);
        }
    }
}
